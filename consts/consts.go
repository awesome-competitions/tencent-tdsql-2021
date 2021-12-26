package consts

const (
	LF                  = byte('\n')
	COMMA               = byte(',')
	K                   = 1024
	M                   = 1024 * K
	G                   = 1024 * M
	FileBufferSize      = 64 * K
	FileSortShardSize   = 32 * M
	FileMergeBufferSize = 32 * M
	InsertBatch         = 16 * K
	FileSortLimit       = 4
	SyncLimit           = 8
)
