package keeper

import (
	"bufio"
	"errors"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

// Excluder decides which folder paths the keeper skips. Paths are relative
// to the folder root, slash-separated. The default reads .stignore and
// .keepignore; wiring can swap in Syncthing's own lib/ignore matcher.
type Excluder interface {
	Excluded(rel string, isDir bool) bool
}

// Names the keeper never keeps or expires.
const (
	StIgnoreFile   = ".stignore"
	KeepIgnoreFile = ".keepignore"
)

// alwaysExcluded reports the paths excluded regardless of ignore files:
// Syncthing's own state and temp files, and keepsync's control files at the
// folder root. Control files are never expired, so a folder cannot lose its
// mapping or ignore rules to expiry.
func alwaysExcluded(rel string, isDir bool) bool {
	base := path.Base(rel)
	if isDir {
		return base == ".stfolder" || base == ".stversions"
	}
	if strings.HasPrefix(base, ".syncthing.") && strings.HasSuffix(base, ".tmp") {
		return true
	}
	if strings.HasPrefix(base, "~syncthing~") && strings.HasSuffix(base, ".tmp") {
		return true
	}
	if !strings.Contains(rel, "/") {
		switch rel {
		case MappingFile, StIgnoreFile, KeepIgnoreFile:
			return true
		}
		if strings.HasPrefix(rel, ".keepsync-") && strings.HasSuffix(rel, ".tmp") {
			return true // WriteMapping's temp file
		}
	}
	// .stfolder can also be a file on some Syncthing versions.
	return base == ".stfolder"
}

// Patterns is an ordered list of ignore patterns in Syncthing's .stignore
// syntax. The first matching pattern decides; "!" patterns re-include.
//
// Supported: "//" comments, blank lines, "!" negation, "(?i)" case folding,
// "(?d)" (accepted, no effect here), a leading "/" anchoring to the folder
// root, "*", "**", "?", "[...]" and "#include <file>". A pattern without "/"
// matches a name at any depth, and a pattern that matches a directory also
// matches everything inside it.
type Patterns struct {
	rules    []rule
	negating bool
}

type rule struct {
	re     *regexp.Regexp
	negate bool
}

// ParsePatterns parses lines of .stignore syntax. includeDir resolves
// "#include" paths; pass "" to ignore includes.
func ParsePatterns(lines []string, includeDir string) (*Patterns, error) {
	p := &Patterns{}
	if err := p.add(lines, includeDir, 0); err != nil {
		return nil, err
	}
	return p, nil
}

// LoadPatterns reads the given ignore files from dir, in order, skipping
// any that do not exist.
func LoadPatterns(dir string, files ...string) (*Patterns, error) {
	p := &Patterns{}
	for _, f := range files {
		lines, err := readLines(filepath.Join(dir, f))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := p.add(lines, dir, 0); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func readLines(name string) ([]string, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lines []string
	s := bufio.NewScanner(f)
	for s.Scan() {
		lines = append(lines, s.Text())
	}
	return lines, s.Err()
}

func (p *Patterns) add(lines []string, includeDir string, depth int) error {
	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		if strings.HasPrefix(line, "#include ") {
			if includeDir == "" || depth > 8 {
				continue
			}
			name := strings.TrimSpace(strings.TrimPrefix(line, "#include "))
			sub, err := readLines(filepath.Join(includeDir, filepath.FromSlash(name)))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if err := p.add(sub, includeDir, depth+1); err != nil {
				return err
			}
			continue
		}
		r, err := compileRule(line)
		if err != nil {
			return err
		}
		if r.negate {
			p.negating = true
		}
		p.rules = append(p.rules, r)
	}
	return nil
}

func compileRule(line string) (rule, error) {
	var r rule
	fold := false
	for {
		switch {
		case strings.HasPrefix(line, "!"):
			r.negate = true
			line = line[1:]
			continue
		case strings.HasPrefix(line, "(?i)"):
			fold = true
			line = line[4:]
			continue
		case strings.HasPrefix(line, "(?d)"):
			line = line[4:]
			continue
		}
		break
	}
	anchored := strings.HasPrefix(line, "/")
	line = strings.TrimPrefix(line, "/")
	line = strings.TrimSuffix(line, "/")
	if !anchored && !strings.Contains(line, "/") {
		// A bare name matches at any depth.
		line = "**/" + line
	}
	var b strings.Builder
	if fold {
		b.WriteString("(?i)")
	}
	b.WriteString("^")
	b.WriteString(globToRegexp(line))
	// Matching a directory matches everything below it.
	b.WriteString("(/.*)?$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return rule{}, err
	}
	r.re = re
	return r, nil
}

func globToRegexp(g string) string {
	var b strings.Builder
	for i := 0; i < len(g); i++ {
		c := g[i]
		switch c {
		case '*':
			if i+1 < len(g) && g[i+1] == '*' {
				i++
				if i+1 < len(g) && g[i+1] == '/' {
					// "**/" matches zero or more leading directories.
					i++
					b.WriteString("(?:.*/)?")
				} else {
					b.WriteString(".*")
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '[':
			j := strings.IndexByte(g[i:], ']')
			if j < 0 {
				b.WriteString(`\[`)
				continue
			}
			class := g[i+1 : i+j]
			if strings.HasPrefix(class, "!") {
				class = "^" + class[1:]
			}
			b.WriteString("[" + strings.ReplaceAll(class, `\`, `\\`) + "]")
			i += j
		case '\\':
			if i+1 < len(g) {
				i++
				b.WriteString(regexp.QuoteMeta(string(g[i])))
			}
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	return b.String()
}

// Excluded reports whether rel is ignored by the patterns.
func (p *Patterns) Excluded(rel string, isDir bool) bool {
	if p == nil {
		return false
	}
	for _, r := range p.rules {
		if r.re.MatchString(rel) {
			return !r.negate
		}
	}
	return false
}

// canSkipDir reports whether an excluded directory can be pruned from the
// walk. With "!" patterns present, something inside may be re-included.
func (p *Patterns) canSkipDir() bool { return p == nil || !p.negating }
