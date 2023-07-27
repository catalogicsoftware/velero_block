package kopia

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/pkg/errors"
)

type BlockOutput struct {
	kopiaOutput *restore.FilesystemOutput
}

var _ restore.Output = BlockOutput{}

const bufferSize = 128 * 1024

func (o BlockOutput) WriteFile(ctx context.Context, relativePath string, remoteFile fs.File) error {
	targetFileName := filepath.Join(o.kopiaOutput.TargetPath, filepath.FromSlash(relativePath))

	targetFileName, err := filepath.EvalSymlinks(targetFileName)
	if err != nil {
		return errors.Wrapf(err, "Unable to evaluate symlinks for %s", targetFileName)
	}

	fileInfo, err := os.Lstat(targetFileName)
	if err != nil {
		return errors.Wrapf(err, "Unable to get the target device information for %s", targetFileName)
	}

	if (fileInfo.Sys().(*syscall.Stat_t).Mode & syscall.S_IFMT) != syscall.S_IFBLK {
		return errors.Errorf("Target file %s is not a block device", targetFileName)
	}

	remoteReader, err := remoteFile.Open(ctx)
	if err != nil {
		return errors.Wrapf(err, "Failed to open remote file %s", remoteFile.Name())
	}
	defer remoteReader.Close()

	targetFile, err := os.Create(targetFileName)
	if err != nil {
		return errors.Wrapf(err, "Failed to open file %s", targetFileName)
	}
	defer targetFile.Close()

	buffer := make([]byte, bufferSize)

	readData := true
	for readData {
		bytesToWrite, err := remoteReader.Read(buffer)
		if err != nil {
			if err != io.EOF {
				return errors.Wrapf(err, "Failed to read data from remote file %s", targetFileName)
			}
			readData = false
		}

		if bytesToWrite > 0 {
			isAllZero := true
			for _, dataByte := range buffer[0:bytesToWrite] {
				if dataByte != 0 {
					isAllZero = false
					break
				}
			}

			if isAllZero {
				if _, err := targetFile.Seek(int64(bytesToWrite), io.SeekCurrent); err != nil {
					return errors.Wrapf(err, "Failed to seek file %s", targetFileName)
				}
				continue
			}

			offset := 0
			for bytesToWrite > 0 {
				if bytesWritten, err := targetFile.Write(buffer[offset:bytesToWrite]); err == nil {
					bytesToWrite -= bytesWritten
					offset += bytesWritten
				} else {
					return errors.Wrapf(err, "Failed to write data to file %s", targetFileName)
				}
			}
		}
	}

	return nil
}

func (o BlockOutput) Parallelizable() bool {
	return o.kopiaOutput.Parallelizable()
}

func (o BlockOutput) BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	return o.kopiaOutput.BeginDirectory(ctx, relativePath, e)
}

func (o BlockOutput) WriteDirEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry, e fs.Directory) error {
	return o.kopiaOutput.WriteDirEntry(ctx, relativePath, de, e)
}

func (o BlockOutput) FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	return o.kopiaOutput.FinishDirectory(ctx, relativePath, e)
}

func (o BlockOutput) FileExists(ctx context.Context, relativePath string, remoteFile fs.File) bool {
	return o.kopiaOutput.FileExists(ctx, relativePath, remoteFile)
}

func (o BlockOutput) CreateSymlink(ctx context.Context, relativePath string, e fs.Symlink) error {
	return o.kopiaOutput.CreateSymlink(ctx, relativePath, e)
}

func (o BlockOutput) SymlinkExists(ctx context.Context, relativePath string, e fs.Symlink) bool {
	return o.kopiaOutput.SymlinkExists(ctx, relativePath, e)
}

func (o BlockOutput) Close(ctx context.Context) error {
	return o.kopiaOutput.Close(ctx)
}
