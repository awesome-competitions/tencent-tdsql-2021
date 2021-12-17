package consts

const (
	LF                        = byte('\n')
	COMMA                     = byte(',')
	UpdateAtColumnName        = "updated_at"
	CreateDatabaseSqlTemplate = "CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_bin';"
	InsertBatch               = 500
)
