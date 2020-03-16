package wrapper

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

func init() {
	sql.Register("postgres-wrapper", &wrapDriver{original: &pq.Driver{}})
}

type wrapDriver struct {
	original driver.Driver
}

type wrapConnector struct {
	original driver.Connector
}

type wrapConn struct {
	original driver.Conn
}

type wrapStmt struct {
	original driver.Stmt
}

func (w *wrapDriver) Open(name string) (driver.Conn, error) {
	original, err := w.original.(driver.Driver).Open(name)
	return &wrapConn{original}, errors.WithStack(err)
}

func (w *wrapConnector) Connect(ctx context.Context) (driver.Conn, error) {
	original, err := w.original.(driver.Connector).Connect(ctx)
	return &wrapConn{original}, errors.WithStack(err)
}

func (w *wrapConnector) Driver() driver.Driver {
	return &wrapDriver{original: w.original.Driver()}
}

func (w *wrapConn) Prepare(query string) (driver.Stmt, error) {
	original, err := w.original.(driver.Conn).Prepare(query)
	return original, errors.WithStack(err)
}

func (w *wrapConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := w.original.(driver.ConnPrepareContext).PrepareContext(ctx, query)
	return stmt, errors.WithStack(err)
}

func (w *wrapConn) Close() error {
	return errors.WithStack(w.original.(driver.Conn).Close())
}

func (w *wrapConn) Begin() (driver.Tx, error) {
	tx, err := w.original.(driver.Conn).Begin()
	return tx, errors.WithStack(err)
}

func (w *wrapConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	res, err := w.original.(driver.ConnBeginTx).BeginTx(ctx, opts)
	return res, errors.WithStack(err)
}

func (w *wrapConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	res, err := w.original.(driver.Execer).Exec(query, args)
	return res, errors.WithStack(err)
}

func (w *wrapConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := w.original.(driver.ExecerContext).ExecContext(ctx, query, args)
	return result, errors.WithStack(err)
}

func (w *wrapConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	res, err := w.original.(driver.Queryer).Query(query, args)
	return res, errors.WithStack(err)
}

func (w *wrapConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := w.original.(driver.QueryerContext).QueryContext(ctx, query, args)
	return rows, errors.WithStack(err)
}

func (w *wrapStmt) Close() error {
	return errors.WithStack(w.original.Close())
}

func (w *wrapStmt) NumInput() int {
	return w.original.NumInput()
}

func (w *wrapStmt) Exec(args []driver.Value) (driver.Result, error) {
	res, err := w.original.Exec(args)
	return res, errors.WithStack(err)
}

func (w *wrapStmt) Query(args []driver.Value) (driver.Rows, error) {
	res, err := w.original.Query(args)
	return res, errors.WithStack(err)
}

func (w *wrapStmt) ColumnConverter(idx int) driver.ValueConverter {
	return w.original.(driver.ColumnConverter).ColumnConverter(idx)
}

func (w *wrapStmt) CheckNamedValue(v *driver.NamedValue) error {
	return errors.WithStack(w.original.(driver.NamedValueChecker).CheckNamedValue(v))
}

func (w *wrapStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	result, err := w.original.(driver.StmtExecContext).ExecContext(ctx, args)
	return result, errors.WithStack(err)
}

func (w *wrapStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := w.original.(driver.StmtQueryContext).QueryContext(ctx, args)
	return rows, errors.WithStack(err)
}

var (
	_ interface {
		driver.Driver
	} = &wrapDriver{}
	_ interface {
		driver.Connector
	} = &wrapConnector{}
	_ interface {
		driver.Conn
		driver.ConnBeginTx
		driver.ConnPrepareContext
		driver.Execer
		driver.ExecerContext
		driver.Queryer
		driver.QueryerContext
	} = &wrapConn{}
	_ interface {
		driver.Stmt
		driver.ColumnConverter
		driver.NamedValueChecker
		driver.StmtExecContext
		driver.StmtQueryContext
	} = &wrapStmt{}
)
