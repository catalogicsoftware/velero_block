/*
Copyright The Velero Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kopia

import (
	"os"
	"syscall"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const ErrNotPermitted = "operation not permitted"

func getLocalBlockEntry(kopiaEntry fs.Entry, log logrus.FieldLogger) (fs.Entry, error) {
	path := kopiaEntry.LocalFilesystemPath()

	fileInfo, err := os.Lstat(path)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get the source device information %s", path)
	}

	if (fileInfo.Sys().(*syscall.Stat_t).Mode & syscall.S_IFMT) != syscall.S_IFBLK {
		return nil, errors.Errorf("source path %s is not a block device", path)
	}

	device, err := os.Open(path)
	if err != nil {
		if os.IsPermission(err) || err.Error() == ErrNotPermitted {
			return nil, errors.Wrapf(err, "no permission to open the source device %s, make sure that node agent is running in privileged mode", path)
		}
		return nil, errors.Wrapf(err, "unable to open the source device %s", path)
	}
	sf := virtualfs.StreamingFileFromReader(kopiaEntry.Name(), device)
	return virtualfs.NewStaticDirectory(kopiaEntry.Name(), []fs.Entry{sf}), nil

}
