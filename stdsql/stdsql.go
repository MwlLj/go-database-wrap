package sql

import (
	"database/sql"
	ser "github.com/MwlLj/go-serde/stdsql_serde"
	"errors"
	"fmt"
	"time"
)

var _ = fmt.Println

type CSql struct {
	db *sql.DB
}

/*
** MaxOpenConnsDefault: 20
** MaxIdleConnsDefault: 10
** ConnMaxLifetimeDefault: 0
 */
func (self *CSql) Connect(driverName string, dial string) (*sql.DB, error) {
	var err error = nil
	self.db, err = sql.Open(driverName, dial)
	self.db.SetMaxOpenConns(20)
	self.db.SetMaxIdleConns(10)
	self.db.SetConnMaxLifetime(60 * time.Second)
	return self.db, err
}

func (self *CSql) FromDB(db *sql.DB) {
	self.db = db
	self.db.SetMaxOpenConns(20)
	self.db.SetMaxIdleConns(10)
	self.db.SetConnMaxLifetime(60 * time.Second)
}

func (self *CSql) Close() error {
	if self.db != nil {
		return self.db.Close()
	} else {
		return errors.New("db is closed")
	}
	return nil
}

func (self *CSql) Std() *sql.DB {
	return self.db
}

func (self *CSql) ExecuteWithTranslate(query string) (*sql.Result, error) {
	if self.db == nil {
		return nil, errors.New("db is closed")
	}
	tx, err := self.db.Begin()
	if err != nil {
		return nil, err
	}
	result, err := tx.Exec(query)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	tx.Commit()
	return &result, nil
}

type MutliSql struct {
	query string
	args  []interface{}
}

func (self *CSql) ExecuteWithTranslateWithArgs(query string, args []interface{}) (*sql.Result, error) {
	if self.db == nil {
		return nil, errors.New("db is closed")
	}
	tx, err := self.db.Begin()
	if err != nil {
		return nil, err
	}
	result, err := tx.Exec(query, args...)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	tx.Commit()
	return &result, nil
}

func (self *CSql) ExecuteWithTranslateWithArgsMutli(querys *[]*MutliSql) error {
	if self.db == nil {
		return errors.New("db is closed")
	}
	tx, err := self.db.Begin()
	if err != nil {
		return err
	}
	for _, v := range *querys {
		q := v
		_, err = tx.Exec(q.query, q.args...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}

func (self *CSql) ExecuteReturnTranslateWithArgs(query string, args []interface{}) (*sql.Result, *sql.Tx, error) {
	if self.db == nil {
		return nil, nil, errors.New("db is closed")
	}
	tx, err := self.db.Begin()
	if err != nil {
		return nil, nil, err
	}
	result, err := tx.Exec(query, args...)
	if err != nil {
		tx.Rollback()
		return nil, nil, err
	}
	return &result, tx, nil
}

func (self *CSql) ExecuteFromTranslate(tx *sql.Tx, query string, args []interface{}) (*sql.Result, error) {
	if self.db == nil {
		return nil, errors.New("db is closed")
	}
	result, err := tx.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return &result, err
}

func (self *CSql) Execute(query string, args ...interface{}) (*sql.Result, error) {
	if self.db == nil {
		return nil, errors.New("db is closed")
	}
	result, err := self.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return &result, err
}

func (self *CSql) Query(query string) (*sql.Rows, error) {
	if self.db == nil {
		return nil, errors.New("db is closed")
	}
	rows, err := self.db.Query(query)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (self *CSql) QueryWithArgs(query string, args []interface{}) (*sql.Rows, error) {
	if self.db == nil {
		return nil, errors.New("db is closed")
	}
	rows, err := self.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (self *CSql) QueryWithArgsFromTranslate(tx *sql.Tx, query string, args []interface{}) (*sql.Rows, error) {
	if self.db == nil {
		return nil, errors.New("db is closed")
	}
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (self *CSql) QueryByTag(output interface{}, query string) error {
	rows, err := self.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = ser.ByTag(rows, output)
	if err != nil {
		return err
	}
	return nil
}

func (self *CSql) QueryByTagWithArgs(output interface{}, query string, args []interface{}) error {
	rows, err := self.QueryWithArgs(query, args)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = ser.ByTag(rows, output)
	if err != nil {
		return err
	}
	return nil
}

func (self *CSql) QueryByTagWithArgsFromTranslate(tx *sql.Tx, output interface{}, query string, args []interface{}) error {
	rows, err := self.QueryWithArgsFromTranslate(tx, query, args)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = ser.ByTag(rows, output)
	if err != nil {
		return err
	}
	return nil
}

func (self *CSql) QueryByTagWithValues(output interface{}, values map[string]interface{}, query string) error {
	rows, err := self.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = ser.ByTagWithValues(rows, output, values)
	if err != nil {
		return err
	}
	return nil
}

func (self *CSql) QueryByTagWithArgsAndValues(output interface{}, values map[string]interface{}, query string, args []interface{}) error {
	rows, err := self.QueryWithArgs(query, args)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = ser.ByTagWithValues(rows, output, values)
	if err != nil {
		return err
	}
	return nil
}

func (self *CSql) Begin() (*sql.Tx, error) {
	return self.db.Begin()
}

func NewSql() *CSql {
	s := CSql{}
	return &s
}
