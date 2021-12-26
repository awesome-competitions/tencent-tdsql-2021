package consts

const (
	LF                  = byte('\n')
	COMMA               = byte(',')
	K                   = 1024
	M                   = 1024 * K
	G                   = 1024 * M
	FileBufferSize      = 256 * K
	FileSortShardSize   = 512 * M
	FileMergeBufferSize = 32 * M
	InsertBatch         = 32 * K
	FileSortLimit       = 4
	SyncLimit           = 8
)
