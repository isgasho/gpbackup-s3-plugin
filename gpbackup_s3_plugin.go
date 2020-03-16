// +build gpbackup_s3_plugin

package main

import (
	"fmt"
	"os"

	"github.com/greenplum-db/gpbackup-s3-plugin/s3plugin"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version",
		Usage: "print version of gpbackup_s3_plugin",
	}
	app.Version = s3plugin.Version
	app.Usage = ""
	app.UsageText = "Not supported as a standalone utility.  This plugin must be used in conjunction with gpbackup and gprestore."

	app.Commands = []cli.Command{
		{
			Name:   "setup_plugin_for_backup",
			Action: s3plugin.SetupPluginForBackup,
			Before: buildBeforeFunc(3, 4),
		},
		{
			Name:   "setup_plugin_for_restore",
			Action: s3plugin.SetupPluginForRestore,
			Before: buildBeforeFunc(3, 4),
		},
		{
			Name:   "cleanup_plugin_for_backup",
			Action: s3plugin.CleanupPlugin,
			Before: buildBeforeFunc(3, 4),
		},
		{
			Name:   "cleanup_plugin_for_restore",
			Action: s3plugin.CleanupPlugin,
			Before: buildBeforeFunc(3, 4),
		},
		{
			Name:   "backup_file",
			Action: s3plugin.BackupFile,
			Before: buildBeforeFunc(2),
		},
		{
			Name:   "backup_directory",
			Action: s3plugin.BackupDirectory,
			Before: buildBeforeFunc(2),
		},
		{
			Name:   "restore_file",
			Action: s3plugin.RestoreFile,
			Before: buildBeforeFunc(2),
		},
		{
			Name:   "restore_directory",
			Action: s3plugin.RestoreDirectory,
			Before: buildBeforeFunc(2),
		},
		{
			Name:   "backup_data",
			Action: s3plugin.BackupData,
			Before: buildBeforeFunc(2),
		},
		{
			Name:   "restore_data",
			Action: s3plugin.RestoreData,
			Before: buildBeforeFunc(2),
		},
		{
			Name:   "plugin_api_version",
			Action: s3plugin.GetAPIVersion,
			Before: buildBeforeFunc(0),
		},
		{
			Name:   "delete_backup",
			Action: s3plugin.Delete,
			Before: buildBeforeFunc(2),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func buildBeforeFunc(expectedNArgs ...int) (beforeFunc cli.BeforeFunc) {
	beforeFunc = func(context *cli.Context) error {
		actualNArg := context.NArg()
		argMatched := false
		for _, expectedNArg := range expectedNArgs {
			if actualNArg == expectedNArg {
				argMatched = true
				break
			}
		}
		if !argMatched {
			return fmt.Errorf("Invalid number of arguments to plugin command. "+
				"Expected %v arguments. Got %d arguments", expectedNArgs, actualNArg)
		} else {
			return nil
		}

	}
	return beforeFunc
}
