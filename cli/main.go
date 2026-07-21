// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/srjn45/rdq/cli/cmd"
)

func main() {
	if err := cmd.Run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
