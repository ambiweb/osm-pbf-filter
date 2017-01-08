package cli

import "os"

// Run parses command line arguments and runs command.
func Run() int {
	env := Env{
		Args:   make([]string, len(os.Args)),
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	copy(env.Args, os.Args)
	return ParseAndRun(env)
}
