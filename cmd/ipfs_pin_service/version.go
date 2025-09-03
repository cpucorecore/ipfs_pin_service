package main

import (
	"fmt"
	"os"
	"runtime"
)

var (
	GitCommit = "unknown"
	GitBranch = "unknown"
	BuildTime = "unknown"
	Version   = "unknown"
)

type Info struct {
	GitCommit string
	GitBranch string
	BuildTime string
	Version   string
	GoVersion string
}

func GetVersion() Info {
	return Info{
		GitCommit: GitCommit,
		GitBranch: GitBranch,
		BuildTime: BuildTime,
		Version:   Version,
		GoVersion: runtime.Version(),
	}
}

func (i Info) String() string {
	return fmt.Sprintf(
		"Version: %s\nGit Branch: %s\nGit Commit: %s\nBuild Time: %s\nGo Version: %s",
		i.Version,
		i.GitBranch,
		i.GitCommit,
		i.BuildTime,
		i.GoVersion,
	)
}

func ShowVersion(showVersion bool) {
	if showVersion {
		fmt.Println(GetVersion())
		os.Exit(0)
	}
}
