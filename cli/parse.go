package cli

import (
	"crypto/md5"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strings"

	yaml "gopkg.in/yaml.v2"

	"github.com/ambiweb/osm-pbf-filter/run"
	"github.com/ambiweb/osm-pbf-filter/tags"
	"github.com/qedus/osmpbf"
	"github.com/syndtr/goleveldb/leveldb"
)

// Env encapsulates command environment.
type Env struct {
	Args   []string
	Stdout io.Writer
	Stderr io.Writer
}

// ParseAndRun parses the environment to create a run.Command and runs it. It returns
// the code that should be used for os.Exit.
func ParseAndRun(env Env) int {
	ui, err := Parse(env)
	if err == flag.ErrHelp {
		fmt.Fprintln(env.Stderr, usage)
		return 0
	}
	if err != nil {
		fmt.Fprintln(env.Stderr, err.Error())
		return 2
	}
	c, err := makeCommand(ui, env)
	if err != nil {
		fmt.Fprintln(env.Stderr, err.Error())
		return 2
	}
	if err := run.Run(c); err != nil {
		fmt.Fprintln(env.Stderr, err.Error())
		return 1
	}
	return 0
}

// UI represents the UI of the CLI.
type UI struct {
	TagsFile string
	Args     []string
}

// Parse converts the program command line.
func Parse(env Env) (*UI, error) {
	ui := &UI{}
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&ui.TagsFile, "tags", "tags.yaml", "")
	if err := fs.Parse(env.Args[1:]); err != nil {
		return nil, err
	}
	ui.Args = fs.Args()
	return ui, nil
}

func makeCommand(ui *UI, env Env) (cmd *run.Command, err error) {
	if len(ui.Args) < 1 {
		return nil, errors.New(usage)
	}
	cmd = &run.Command{Stdout: env.Stdout}
	if cmd.PBFDecoder, err = makePBFDecoder(ui.Args); err != nil {
		return nil, err
	}
	if cmd.LevelDB, err = makeLevelDB(ui.Args); err != nil {
		return nil, err
	}
	if cmd.TagsMatcher, err = makeTagsMatcher(ui.TagsFile); err != nil {
		return nil, err
	}

	return cmd, nil
}

func makePBFDecoder(files []string) (*osmpbf.Decoder, error) {
	rs := make([]io.Reader, len(files))
	for i, s := range files {
		f, err := os.Open(s)
		if err != nil {
			return nil, err
		}
		rs[i] = f
	}
	pbfReader := io.MultiReader(rs...)
	dec := osmpbf.NewDecoder(pbfReader)
	// use more memory from the start, it is faster
	dec.SetBufferSize(osmpbf.MaxBlobSize)
	// start decoding with several goroutines, it is faster
	if err := dec.Start(runtime.GOMAXPROCS(-1)); err != nil {
		return nil, err
	}
	return dec, nil
}

func makeLevelDB(files []string) (*leveldb.DB, error) {
	data := []byte(strings.Join(files, ""))
	path := fmt.Sprintf("%x.db", md5.Sum(data))
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func makeTagsMatcher(file string) (tags.Matcher, error) {
	var tagsMatcher tags.Matcher
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(b, &tagsMatcher); err != nil {
		return nil, err
	}
	return tagsMatcher, nil
}

const usage = `Usage:
osm-pbf-filter [OPTIONS] FILE.pbf

Options:
  -tags YAML file with tags to match specified. Default 'tags.yaml' in current
        directory.
`
