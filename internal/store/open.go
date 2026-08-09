package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"modernc.org/sqlite"

	"github.com/ryancswallace/jobman/internal/model"
)

const (
	// DatabaseFilename is the fixed metadata database name inside a state root.
	DatabaseFilename   = "jobman.db"
	defaultBusyTimeout = 5 * time.Second
	minimumSQLiteMajor = 3
	minimumSQLiteMinor = 51
	minimumSQLitePatch = 3
)

// Options configures a metadata store.
type Options struct {
	StateDir      string
	BusyTimeout   time.Duration
	JobmanVersion string
	Now           func() time.Time
	EventIDs      EventIDSource
	Random        io.Reader
}

// EventIDSource supplies identifiers for append-only transition events.
type EventIDSource interface {
	NewEventID() (model.EventID, error)
}

// Store is a single-process handle to Jobman's per-user metadata database.
// Each process deliberately owns only one pooled physical connection.
type Store struct {
	db             *sql.DB
	stateDir       string
	databasePath   string
	sqliteVersion  string
	busyTimeout    time.Duration
	now            func() time.Time
	jobmanVersion  string
	eventIDs       EventIDSource
	random         io.Reader
	fingerprintKey [32]byte
	lastBackup     string
	// supervisorLeaseMu serializes this supervisor handle's lease renewal
	// against terminal transitions that release the same supervisor revision.
	supervisorLeaseMu sync.Mutex
}

// Open creates or validates a private state directory, opens its SQLite
// database, and applies all known migrations.
//
//nolint:gocognit,cyclop // Opening owns ordered validation and cleanup for each acquired resource.
func Open(ctx context.Context, options Options) (*Store, error) {
	if ctx == nil {
		return nil, errors.New("open store: nil context")
	}

	var stateDir, databasePath string
	err := withStorePreparationLock(ctx, options.StateDir, func() error {
		preparedStateDir, prepareErr := prepareStateDir(options.StateDir)
		if prepareErr != nil {
			return prepareErr
		}
		stateDir = preparedStateDir
		if filesystemErr := validateStateFilesystem(stateDir); filesystemErr != nil {
			return filesystemErr
		}

		databasePath = filepath.Join(stateDir, DatabaseFilename)
		if databaseErr := prepareDatabaseFile(databasePath); databaseErr != nil {
			return databaseErr
		}
		// Validate existing journals before SQLite can read them. Journals created
		// by this open are hardened immediately after initialization below.
		return validateExistingDatabaseSidecars(databasePath)
	})
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}

	busyTimeout := options.BusyTimeout
	if busyTimeout == 0 {
		busyTimeout = defaultBusyTimeout
	}
	if busyTimeout < 0 || busyTimeout > time.Duration(int(^uint32(0)>>1))*time.Millisecond {
		return nil, fmt.Errorf("open store: invalid busy timeout %s", busyTimeout)
	}

	now := options.Now
	if now == nil {
		now = time.Now
	}
	eventIDs := options.EventIDs
	if eventIDs == nil {
		generator, generatorErr := model.NewUUIDv7Generator(now, rand.Reader)
		if generatorErr != nil {
			return nil, fmt.Errorf("open store: construct event ID source: %w", generatorErr)
		}
		eventIDs = generator
	}
	randomSource := options.Random
	if randomSource == nil {
		randomSource = rand.Reader
	}

	db, err := sql.Open("sqlite", sqliteDSN(databasePath, busyTimeout))
	if err != nil {
		return nil, fmt.Errorf("open store database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	store := &Store{
		db:            db,
		stateDir:      stateDir,
		databasePath:  databasePath,
		busyTimeout:   busyTimeout,
		now:           now,
		jobmanVersion: options.JobmanVersion,
		eventIDs:      eventIDs,
		random:        randomSource,
	}

	if err := store.initialize(ctx); err != nil {
		closeErr := db.Close()
		if closeErr != nil {
			return nil, errors.Join(err, fmt.Errorf("close failed store: %w", closeErr))
		}

		return nil, err
	}
	securityErr := withStorePreparationLock(ctx, options.StateDir, func() error {
		if validateErr := validateDatabaseFile(databasePath); validateErr != nil {
			return validateErr
		}
		if hardenErr := hardenDatabaseSidecars(databasePath); hardenErr != nil {
			return hardenErr
		}

		return validateDatabaseSidecars(databasePath)
	})
	if securityErr != nil {
		closeErr := db.Close()
		if closeErr != nil {
			return nil, errors.Join(securityErr, fmt.Errorf("close unsafe store: %w", closeErr))
		}

		return nil, fmt.Errorf("open store: %w", securityErr)
	}

	return store, nil
}

func (s *Store) initialize(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect to store database: %w", classifySQLite("connect to store database", err))
	}

	if err := s.verifySQLiteVersion(ctx); err != nil {
		return err
	}
	if err := s.enableAndVerifyWAL(ctx); err != nil {
		return err
	}
	if err := s.verifyConnectionPragmas(ctx); err != nil {
		return err
	}
	if err := s.backupBeforeUpgrade(ctx); err != nil {
		return err
	}
	if err := s.migrate(ctx); err != nil {
		return err
	}
	if err := s.ensureFingerprintKey(ctx); err != nil {
		return err
	}

	return s.verifySchema(ctx)
}

func sqliteDSN(databasePath string, busyTimeout time.Duration) string {
	query := make(url.Values)
	query.Add("_pragma", "busy_timeout("+strconv.FormatInt(busyTimeout.Milliseconds(), 10)+")")
	query.Add("_pragma", "foreign_keys(ON)")
	query.Add("_pragma", "synchronous(FULL)")
	query.Set("_txlock", "immediate")

	return (&url.URL{
		Scheme:   "file",
		Path:     sqliteURIPath(databasePath),
		RawQuery: query.Encode(),
	}).String()
}

func (s *Store) verifySQLiteVersion(ctx context.Context) error {
	var version string
	if err := s.db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return fmt.Errorf("read SQLite version: %w", classifySQLite("read SQLite version", err))
	}

	major, minor, patch, err := parseSQLiteVersion(version)
	if err != nil {
		return fmt.Errorf("read SQLite version: %w", err)
	}
	if versionLessThan(major, minor, patch, minimumSQLiteMajor, minimumSQLiteMinor, minimumSQLitePatch) {
		return &SchemaError{Reason: fmt.Sprintf(
			"SQLite %s is older than required version %d.%d.%d",
			version,
			minimumSQLiteMajor,
			minimumSQLiteMinor,
			minimumSQLitePatch,
		)}
	}
	s.sqliteVersion = version

	return nil
}

func parseSQLiteVersion(version string) (major, minor, patch int, err error) {
	parts := strings.Split(version, ".")
	if len(parts) < 3 {
		return 0, 0, 0, fmt.Errorf("invalid SQLite version %q", version)
	}

	values := [3]int{}
	for index := range values {
		value, err := strconv.Atoi(parts[index])
		if err != nil || value < 0 {
			return 0, 0, 0, fmt.Errorf("invalid SQLite version %q", version)
		}
		values[index] = value
	}

	return values[0], values[1], values[2], nil
}

func versionLessThan(major, minor, patch, wantMajor, wantMinor, wantPatch int) bool {
	if major != wantMajor {
		return major < wantMajor
	}
	if minor != wantMinor {
		return minor < wantMinor
	}

	return patch < wantPatch
}

func (s *Store) enableAndVerifyWAL(ctx context.Context) error {
	deadline := time.Now().Add(s.busyTimeout)
	for {
		var journalMode string
		err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode)
		if err == nil {
			if !strings.EqualFold(journalMode, "wal") {
				return fmt.Errorf("enable SQLite WAL: database returned journal mode %q", journalMode)
			}

			return nil
		}
		classified := classifySQLite("enable SQLite WAL", err)
		if !errors.Is(classified, ErrBusy) || !time.Now().Before(deadline) {
			return fmt.Errorf("enable SQLite WAL: %w", classified)
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("enable SQLite WAL: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func (s *Store) verifyConnectionPragmas(ctx context.Context) error {
	checks := []struct {
		name string
		want int
	}{
		{name: "foreign_keys", want: 1},
		{name: "synchronous", want: 2},
		{name: "busy_timeout", want: int(s.busyTimeout.Milliseconds())},
	}

	for _, check := range checks {
		var got int
		query := "PRAGMA " + check.name // #nosec G202 -- names are fixed constants above.
		if err := s.db.QueryRowContext(ctx, query).Scan(&got); err != nil {
			return fmt.Errorf("verify SQLite %s pragma: %w", check.name, err)
		}
		if got != check.want {
			return fmt.Errorf("verify SQLite %s pragma: got %d, want %d", check.name, got, check.want)
		}
	}

	return nil
}

func classifySQLite(operation string, err error) error {
	if err == nil {
		return nil
	}

	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		const (
			sqliteBusy   = 5
			sqliteLocked = 6
		)
		if primaryCode := sqliteErr.Code() & 0xff; primaryCode == sqliteBusy || primaryCode == sqliteLocked {
			return &BusyError{Operation: operation, Err: err}
		}
	}

	return err
}

// Close releases this process's database connection.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}

	return s.db.Close()
}

// StateDir returns the canonical absolute state root.
func (s *Store) StateDir() string {
	return s.stateDir
}

// DatabasePath returns the canonical absolute metadata database path.
func (s *Store) DatabasePath() string {
	return s.databasePath
}

// SQLiteVersion returns the version reported by the bundled SQLite library.
func (s *Store) SQLiteVersion() string {
	return s.sqliteVersion
}

// LastBackupPath reports the migration backup created while opening this
// store, or an empty string when no upgrade was required.
func (s *Store) LastBackupPath() string { return s.lastBackup }
