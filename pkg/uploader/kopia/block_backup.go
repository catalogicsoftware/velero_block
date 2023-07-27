package kopia

import (
	"context"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// deviceEntry implements fs.File interface
type deviceEntry struct {
	path       string
	size       int64
	kopiaEntry fs.Entry
	log        logrus.FieldLogger
}

// deviceReader implements fs.Reader interface
type deviceReader struct {
	e    *deviceEntry
	file *os.File
}

var _ fs.Reader = deviceReader{}
var _ fs.File = deviceEntry{}

const ErrNotPermitted = "operation not permitted"

func getLocalBlockEntry(kopiaEntry fs.Entry, log logrus.FieldLogger) (fs.Entry, error) {
	path := kopiaEntry.LocalFilesystemPath()

	fileInfo, err := os.Lstat(path)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to get the source device information %s", path)
	}

	if (fileInfo.Sys().(*syscall.Stat_t).Mode & syscall.S_IFMT) != syscall.S_IFBLK {
		return nil, errors.Errorf("Source path %s is not a block device", path)
	}

	device, err := os.Open(path)
	if err != nil {
		if os.IsPermission(err) || err.Error() == ErrNotPermitted {
			return nil, errors.Wrapf(err, "No permission to open the source device %s, make sure that node agent is running in privileged mode", path)
		}
		return nil, errors.Wrapf(err, "Unable to open the source device %s", path)
	}
	defer device.Close()

	size, err := device.Seek(0, io.SeekEnd) // seek to the end of block device to discover its size
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to get the source device size %s", path)
	}

	return fs.File(&deviceEntry{
		path:       path,
		size:       size,
		kopiaEntry: kopiaEntry,
	}), nil
}

// deviceEntry implementation

func (e deviceEntry) Open(ctx context.Context) (fs.Reader, error) {
	device, err := os.Open(e.path)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to open the source device %s", e.path)
	}
	return &deviceReader{
		e:    &e,
		file: device,
	}, nil
}

func (e deviceEntry) Owner() fs.OwnerInfo {
	return e.kopiaEntry.Owner()
}

func (e deviceEntry) Device() fs.DeviceInfo {
	return e.kopiaEntry.Device()
}

func (e deviceEntry) LocalFilesystemPath() string {
	return e.kopiaEntry.LocalFilesystemPath()
}

func (e deviceEntry) Name() string {
	return e.kopiaEntry.Name()
}

func (e deviceEntry) Mode() os.FileMode {
	return e.kopiaEntry.Mode()
}

func (e deviceEntry) Size() int64 {
	return e.size
}

func (e deviceEntry) ModTime() time.Time {
	return e.kopiaEntry.ModTime()
}

func (e deviceEntry) IsDir() bool {
	return e.kopiaEntry.IsDir()
}

func (e deviceEntry) Sys() any {
	return e.kopiaEntry.Sys()
}

func (e deviceEntry) Close() {
	e.kopiaEntry.Close()
}

// deviceReader implementation

func (r deviceReader) Entry() (fs.Entry, error) {
	return r.e, nil
}

func (e deviceReader) Close() error {
	return e.file.Close()
}

func (e deviceReader) Read(buffer []byte) (int, error) {
	return e.file.Read(buffer)
}

func (e deviceReader) Seek(offset int64, whence int) (int64, error) {
	return e.file.Seek(offset, whence)
}
