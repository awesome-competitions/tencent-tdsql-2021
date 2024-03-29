package consts

const (
	Dir = "D:\\workspace-tencent\\tmp1\\"
	//Dir                 = "/mnt/"
	LF                  = byte('\n')
	COMMA               = byte(',')
	K                   = 1024
	M                   = 1024 * K
	G                   = 1024 * M
	FileBufferSize      = 64 * K
	FileSortShardSize   = 16 * M
	FileMergeBufferSize = 32 * M
	InsertBatch         = 256 * K
	FileSortLimit       = 1
	SyncLimit           = 28
	PreparedBatch       = 1
)
