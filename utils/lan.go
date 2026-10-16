// Copyright (c) 2022-2024 The authors (see the AUTHORS file)
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package utils

import (
	"github.com/juju/errors"
	"net"
)

func DiscoverSystemNics() ([]string, error) {
	var out []string
	itfs, err := net.Interfaces()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, itf := range itfs {
		out = append(out, itf.Name)
	}
	return out, nil
}
