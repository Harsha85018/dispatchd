package store

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Harsha85018/dispatchd/internal/job"
)

//go:embed migrations/001_jobs.sql
var schemaSQL string

// PostgresStore keeps job state in Postgres instead of an in-process map
// plus a local write-ahead log. Durability comes from Postgres commits,
// and exclusive leasing from row-level locks, so no server-side mutex is
// needed and several server replicas can share one database.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

const jobColumns = `id, type, payload, status, depends_on, attempts, max_attempts,
	leased_by, lease_token, lease_expiry, error, created_at, updated_at`

// scanJob reads one row into a Job. Nullable lease columns come back as
// pointers so an unleased job keeps them empty rather than zero-valued.
func scanJob(row pgx.Row) (*job.Job, error) {
	var j job.Job
	var leasedBy, token *string
	var expiry *time.Time

	err := row.Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.DependsOn,
		&j.Attempts, &j.MaxAttempts,
		&leasedBy, &token, &expiry,
		&j.Error, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if leasedBy != nil {
		j.LeasedBy = *leasedBy
	}
	if token != nil {
		j.LeaseToken = *token
	}
	j.LeaseExpiry = expiry

	return &j, nil
}

func (s *PostgresStore) Put(j *job.Job) error {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobs (`+jobColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			attempts = EXCLUDED.attempts,
			leased_by = EXCLUDED.leased_by,
			lease_token = EXCLUDED.lease_token,
			lease_expiry = EXCLUDED.lease_expiry,
			error = EXCLUDED.error,
			updated_at = EXCLUDED.updated_at`,
		j.ID, j.Type, j.Payload, j.Status, j.DependsOn,
		j.Attempts, j.MaxAttempts,
		nullable(j.LeasedBy), nullable(j.LeaseToken), j.LeaseExpiry,
		j.Error, j.CreatedAt, j.UpdatedAt,
	)
	return err
}

func (s *PostgresStore) Get(id string) *job.Job {
	row := s.pool.QueryRow(context.Background(),
		`SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)

	j, err := scanJob(row)
	if err != nil {
		return nil
	}
	return j
}

func (s *PostgresStore) All() []*job.Job {
	rows, err := s.pool.Query(context.Background(),
		`SELECT `+jobColumns+` FROM jobs ORDER BY created_at`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var out []*job.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return out
		}
		out = append(out, j)
	}
	return out
}

// Lease claims one ready job in a single statement. FOR UPDATE SKIP LOCKED
// lets concurrent callers each take a different row instead of queuing on
// the same one, which is what removes the single-mutex bottleneck the
// file-backed store had.
func (s *PostgresStore) Lease(workerID string, duration time.Duration) (*job.Job, error) {
	token, err := newLeaseToken()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	row := s.pool.QueryRow(context.Background(), `
		UPDATE jobs SET
			status = 'leased',
			leased_by = $1,
			lease_token = $2,
			lease_expiry = $3,
			attempts = attempts + 1,
			updated_at = $4
		WHERE id = (
			SELECT j.id FROM jobs j
			WHERE j.status = 'pending'
			  AND NOT EXISTS (
				SELECT 1 FROM unnest(j.depends_on) AS dep_id
				WHERE NOT EXISTS (
					SELECT 1 FROM jobs d
					WHERE d.id = dep_id AND d.status = 'succeeded'
				)
			  )
			ORDER BY j.created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING `+jobColumns,
		workerID, token, now.Add(duration), now,
	)

	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // nothing ready right now
	}
	if err != nil {
		return nil, err
	}
	return j, nil
}

// Renew extends a lease the worker still holds. The token check is what
// stops a worker that already lost and re-acquired this job from renewing
// with a stale claim.
func (s *PostgresStore) Renew(id, workerID, token string, duration time.Duration) error {
	now := time.Now().UTC()
	tag, err := s.pool.Exec(context.Background(), `
		UPDATE jobs SET lease_expiry = $1, updated_at = $2
		WHERE id = $3 AND status = 'leased'
		  AND leased_by = $4 AND lease_token = $5`,
		now.Add(duration), now, id, workerID, token,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return s.missingOrLost(id)
	}
	return nil
}

// Complete records the outcome of a leased job: succeeded, requeued for
// another attempt, or failed for good once attempts are exhausted.
func (s *PostgresStore) Complete(id, workerID, token string, success bool, errMsg string) error {
	now := time.Now().UTC()
	tag, err := s.pool.Exec(context.Background(), `
		UPDATE jobs SET
			status = CASE
				WHEN $1 THEN 'succeeded'
				WHEN attempts < max_attempts THEN 'pending'
				ELSE 'failed'
			END,
			error = CASE WHEN $1 THEN '' ELSE $2 END,
			leased_by = NULL,
			lease_token = NULL,
			lease_expiry = NULL,
			updated_at = $3
		WHERE id = $4 AND leased_by = $5 AND lease_token = $6`,
		success, errMsg, now, id, workerID, token,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return s.missingOrLost(id)
	}
	return nil
}

// ReapExpired returns jobs whose leases ran out to the queue, which is how
// work is recovered from workers that died mid-job.
func (s *PostgresStore) ReapExpired() ([]string, error) {
	now := time.Now().UTC()
	rows, err := s.pool.Query(context.Background(), `
		UPDATE jobs SET
			status = CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END,
			error = CASE WHEN attempts < max_attempts
				THEN 'lease expired, requeued'
				ELSE 'lease expired after max attempts' END,
			leased_by = NULL,
			lease_token = NULL,
			lease_expiry = NULL,
			updated_at = $1
		WHERE status = 'leased' AND lease_expiry < $1
		RETURNING id`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reaped []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return reaped, err
		}
		reaped = append(reaped, id)
	}
	return reaped, rows.Err()
}

// missingOrLost distinguishes "no such job" from "someone else holds it",
// so callers can tell a bad id apart from a lost lease.
func (s *PostgresStore) missingOrLost(id string) error {
	var exists bool
	err := s.pool.QueryRow(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}
	return ErrLeaseLost
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}