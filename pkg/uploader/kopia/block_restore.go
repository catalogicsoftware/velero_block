package kopia

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/pkg/errors"
)

type BlockOutput struct {
	*restore.FilesystemOutput
}

var _ restore.Output = &BlockOutput{}

const bufferSize = 128 * 1024

func (o *BlockOutput) WriteFile(ctx context.Context, relativePath string, remoteFile fs.File) error {

	targetFileName, err := filepath.EvalSymlinks(o.TargetPath)
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

func (o *BlockOutput) BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	targetFileName, err := filepath.EvalSymlinks(o.TargetPath)
	if err != nil {
		return errors.Wrapf(err, "Unable to evaluate symlinks for %s", targetFileName)
	}

	fileInfo, err := os.Lstat(targetFileName)
	if err != nil {
		return errors.Wrapf(err, "Unable to get the target device information for %s", o.TargetPath)
	}

	if (fileInfo.Sys().(*syscall.Stat_t).Mode & syscall.S_IFMT) != syscall.S_IFBLK {
		return errors.Errorf("Target file %s is not a block device", o.TargetPath)
	}

	return nil
}
