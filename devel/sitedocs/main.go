// Package main builds the deterministic source tree for the Jobman website.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"

	"github.com/ryancswallace/jobman/jobman"
)

const (
	defaultSiteSource = "site"
	defaultSiteOutput = "site-build"
	referenceParent   = "Reference"
	operationsParent  = "Operations"
	projectParent     = "Project"
	sharedModeSiteURL = "({{ site.baseurl }}/guides/shared-mode/)"
)

type publishedPage struct {
	source      string
	destination string
	title       string
	parent      string
	grandParent string
	permalink   string
	navOrder    int
}

var publishedPages = []publishedPage{
	{source: "docs/CONTAINERS.md", destination: "guides/containers.md", title: "Containers", parent: "User guides", permalink: "/guides/containers/", navOrder: 9},
	{source: "docs/SHARED_MODE.md", destination: "guides/shared-mode.md", title: "Shared-mode preview", parent: "User guides", permalink: "/guides/shared-mode/", navOrder: 10},
	{source: "docs/CONFIGURATION.md", destination: "reference/configuration.md", title: "Configuration schema", parent: referenceParent, permalink: "/reference/configuration/", navOrder: 2},
	{source: "docs/COMPATIBILITY.md", destination: "reference/compatibility.md", title: "Compatibility contract", parent: referenceParent, permalink: "/reference/compatibility/", navOrder: 5},
	{source: "docs/design/PLATFORM_CAPABILITIES.md", destination: "reference/platforms.md", title: "Platform support", parent: referenceParent, permalink: "/reference/platforms/", navOrder: 4},
	{source: "docs/UPGRADING.md", destination: "operations/upgrading.md", title: "Upgrading and restoring", parent: operationsParent, permalink: "/operations/upgrading/", navOrder: 3},
	{source: "SECURITY.md", destination: "operations/security.md", title: "Security policy", parent: operationsParent, permalink: "/operations/security/", navOrder: 4},
	{source: "SUPPORT.md", destination: "operations/support.md", title: "Support policy", parent: operationsParent, permalink: "/operations/support/", navOrder: 5},
	{source: "CONTRIBUTING.md", destination: "project/contributing.md", title: "Contributing", parent: projectParent, permalink: "/project/contributing/", navOrder: 2},
	{source: "CODE_OF_CONDUCT.md", destination: "project/code-of-conduct.md", title: "Code of conduct", parent: projectParent, permalink: "/project/code-of-conduct/", navOrder: 3},
	{source: "RELEASE.md", destination: "project/releasing.md", title: "Release process", parent: projectParent, permalink: "/project/releasing/", navOrder: 4},
	{source: "docs/DOGFOOD.md", destination: "project/dogfood.md", title: "Dogfood runbook", parent: projectParent, permalink: "/project/dogfood/", navOrder: 5},
	{source: "CHANGELOG.md", destination: "project/changelog.md", title: "Changelog", parent: projectParent, permalink: "/project/changelog/", navOrder: 6},
	{source: "docs/design/DISTRIBUTED_MODE.md", destination: "project/distributed-mode.md", title: "Distributed-mode design", parent: projectParent, permalink: "/project/distributed-mode/", navOrder: 7},
}

var demoRecordings = []string{"basic.webm", "dependencies.webm", "input.webm", "retries.webm", "timeouts.webm"}

var canonicalLinkReplacements = map[string]string{
	"(UPGRADING.md)":                     "({{ site.baseurl }}/operations/upgrading/)",
	"(CODE_OF_CONDUCT.md)":               "({{ site.baseurl }}/project/code-of-conduct/)",
	"(SECURITY.md)":                      "({{ site.baseurl }}/operations/security/)",
	"(CHANGELOG.md)":                     "({{ site.baseurl }}/project/changelog/)",
	"(LICENSE)":                          "(https://github.com/ryancswallace/jobman/blob/main/LICENSE)",
	"(docs/CONTAINERS.md)":               "({{ site.baseurl }}/guides/containers/)",
	"(docs/SHARED_MODE.md)":              sharedModeSiteURL,
	"(SHARED_MODE.md)":                   sharedModeSiteURL,
	"(AGENT.md)":                         "(https://github.com/ryancswallace/jobman/blob/main/docs/AGENT.md)",
	"(design/DISTRIBUTED_MODE.md)":       "({{ site.baseurl }}/project/distributed-mode/)",
	"(../SHARED_MODE.md)":                sharedModeSiteURL,
	"(SPEC.md)":                          "(https://github.com/ryancswallace/jobman/blob/main/docs/design/SPEC.md)",
	"(SUPPORT.md)":                       "({{ site.baseurl }}/operations/support/)",
	"(SPEC.md#17-platform-requirements)": "({{ site.baseurl }}/project/design/#platform-and-process-model)",
	"(adr/0001-per-job-supervisor.md)":   "({{ site.baseurl }}/project/design/#architectural-decisions)",
	"(../DOGFOOD.md)":                    "({{ site.baseurl }}/project/dogfood/)",
	"(../../SECURITY.md)":                "({{ site.baseurl }}/operations/security/)",
	"(../../SUPPORT.md)":                 "({{ site.baseurl }}/operations/support/)",
	"[dogfood runbook]: docs/DOGFOOD.md": "[dogfood runbook]: {{ site.baseurl }}/project/dogfood/",
	"[platform capability record]: docs/design/PLATFORM_CAPABILITIES.md": "[platform capability record]: {{ site.baseurl }}/reference/platforms/",
	"[sample configuration]: ../etc/jobman/jobman.yml":                   "[sample configuration]: {{ site.baseurl }}/assets/examples/jobman.yml",
}

func generateSite(repositoryRoot, outputRoot string) error {
	sourceRoot := filepath.Join(repositoryRoot, defaultSiteSource)
	if filepath.Clean(sourceRoot) == filepath.Clean(outputRoot) {
		return errors.New("site output must differ from authored source")
	}
	if err := os.RemoveAll(outputRoot); err != nil {
		return fmt.Errorf("remove previous site output: %w", err)
	}
	if err := copyAuthoredSite(sourceRoot, outputRoot); err != nil {
		return err
	}
	for _, page := range publishedPages {
		if err := publishCanonicalPage(repositoryRoot, outputRoot, page); err != nil {
			return err
		}
	}
	if err := copyFile(
		filepath.Join(repositoryRoot, "etc", "jobman", "jobman.yml"),
		filepath.Join(outputRoot, "assets", "examples", "jobman.yml"),
	); err != nil {
		return fmt.Errorf("publish sample configuration: %w", err)
	}
	for _, name := range []string{"logo.svg", "logo-dark.svg", "logo-dark-transparent.svg", "favicon.svg", "favicon-dark.svg"} {
		if err := copyFile(
			filepath.Join(repositoryRoot, "assets", name),
			filepath.Join(outputRoot, "assets", "images", name),
		); err != nil {
			return fmt.Errorf("publish site image %s: %w", name, err)
		}
	}
	for _, name := range demoRecordings {
		if err := copyFile(
			filepath.Join(repositoryRoot, "docs", "screencaps", "webm", name),
			filepath.Join(outputRoot, "assets", "videos", name),
		); err != nil {
			return fmt.Errorf("publish demo recording %s: %w", name, err)
		}
	}
	if err := generateCommandReference(filepath.Join(outputRoot, "reference", "commands")); err != nil {
		return err
	}
	if err := validateSite(outputRoot); err != nil {
		return fmt.Errorf("validate staged site: %w", err)
	}

	return nil
}

func copyAuthoredSite(sourceRoot, outputRoot string) error {
	return filepath.WalkDir(sourceRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}
		if relative == "." {
			return os.MkdirAll(outputRoot, 0o750)
		}
		if entry.IsDir() {
			if entry.Name() == "_site" {
				return filepath.SkipDir
			}

			return os.MkdirAll(filepath.Join(outputRoot, relative), 0o750)
		}
		if relative == "README.md" {
			return nil
		}

		return copyFile(path, filepath.Join(outputRoot, relative))
	})
}

func copyFile(source, destination string) error {
	contents, err := os.ReadFile(source)
	if err != nil {
		return fmt.Errorf("read %s: %w", source, err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return fmt.Errorf("create directory for %s: %w", destination, err)
	}
	// #nosec G306,G703 -- staged documentation is a public deployment input at a repository-controlled path.
	if err := os.WriteFile(destination, contents, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", destination, err)
	}

	return nil
}

func publishCanonicalPage(repositoryRoot, outputRoot string, page publishedPage) error {
	source := filepath.Join(repositoryRoot, filepath.FromSlash(page.source))
	body, err := os.ReadFile(source)
	if err != nil {
		return fmt.Errorf("read canonical page %s: %w", page.source, err)
	}
	contents := string(body)
	for old, replacement := range canonicalLinkReplacements {
		contents = strings.ReplaceAll(contents, old, replacement)
	}
	destination := filepath.Join(outputRoot, filepath.FromSlash(page.destination))
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return fmt.Errorf("create canonical page directory: %w", err)
	}
	contents = pageFrontMatter(page.title, page.parent, page.grandParent, page.permalink, page.navOrder, false) + contents
	// #nosec G306,G703 -- staged documentation is a public deployment input at a static path.
	if err := os.WriteFile(destination, []byte(contents), 0o644); err != nil {
		return fmt.Errorf("write canonical page %s: %w", page.destination, err)
	}

	return nil
}

func generateCommandReference(outputRoot string) error {
	root := jobman.NewCommand()
	root.InitDefaultCompletionCmd()
	disableAutoGenerationDates(root)
	commands := availableCommands(root)
	for index, command := range commands {
		if err := writeCommandPage(outputRoot, command, index+1); err != nil {
			return err
		}
	}

	return nil
}

func disableAutoGenerationDates(command *cobra.Command) {
	command.DisableAutoGenTag = true
	for _, child := range command.Commands() {
		disableAutoGenerationDates(child)
	}
}

func availableCommands(root *cobra.Command) []*cobra.Command {
	commands := []*cobra.Command{root}
	var visit func(*cobra.Command)
	visit = func(parent *cobra.Command) {
		children := append([]*cobra.Command(nil), parent.Commands()...)
		sort.Slice(children, func(left, right int) bool { return children[left].CommandPath() < children[right].CommandPath() })
		for _, child := range children {
			if !child.IsAvailableCommand() || child.IsAdditionalHelpTopicCommand() {
				continue
			}
			commands = append(commands, child)
			visit(child)
		}
	}
	visit(root)

	return commands
}

func writeCommandPage(outputRoot string, command *cobra.Command, navOrder int) error {
	segments := commandSegments(command.CommandPath())
	directory := filepath.Join(append([]string{outputRoot}, segments...)...)
	if len(segments) == 0 {
		directory = outputRoot
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create command reference directory: %w", err)
	}
	var generated bytes.Buffer
	if err := doc.GenMarkdownCustom(command, &generated, commandLink); err != nil {
		return fmt.Errorf("generate reference for %s: %w", command.CommandPath(), err)
	}
	body := polishCommandMarkdown(generated.String())
	title := strings.TrimPrefix(command.CommandPath(), "jobman ")
	parent := "Command-line reference"
	grandParent := "Reference"
	hasChildren := false
	if command == command.Root() {
		title = "Command-line reference"
		parent = "Reference"
		grandParent = ""
		hasChildren = true
	}
	contents := pageFrontMatter(title, parent, grandParent, commandPermalink(command.CommandPath()), navOrder, hasChildren) + body
	path := filepath.Join(directory, "index.md")
	// #nosec G306 -- generated command references are public deployment inputs.
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		return fmt.Errorf("write command reference %s: %w", path, err)
	}

	return nil
}

func commandSegments(commandPath string) []string {
	parts := strings.Fields(commandPath)
	if len(parts) <= 1 {
		return nil
	}

	return parts[1:]
}

func commandPermalink(commandPath string) string {
	segments := commandSegments(commandPath)
	if len(segments) == 0 {
		return "/reference/commands/"
	}

	return "/reference/commands/" + strings.Join(segments, "/") + "/"
}

func commandLink(filename string) string {
	commandPath := strings.TrimSuffix(filename, filepath.Ext(filename))
	commandPath = strings.ReplaceAll(commandPath, "_", " ")

	return "{{ site.baseurl }}" + commandPermalink(commandPath)
}

func polishCommandMarkdown(markdown string) string {
	replacements := []struct{ old, replacement string }{
		{old: "## ", replacement: "# "},
		{old: "### Synopsis", replacement: "## Synopsis"},
		{old: "### Examples", replacement: "## Examples"},
		{old: "### Options inherited from parent commands", replacement: "## Global options"},
		{old: "### Options", replacement: "## Options"},
		{old: "### SEE ALSO", replacement: "## Related commands"},
	}
	for _, item := range replacements {
		markdown = strings.Replace(markdown, item.old, item.replacement, 1)
	}

	return markdown
}

func pageFrontMatter(title, parent, grandParent, permalink string, navOrder int, hasChildren bool) string {
	var output strings.Builder
	output.WriteString("---\nlayout: default\ntitle: ")
	output.WriteString(strconv.Quote(title))
	output.WriteString("\n")
	if parent != "" {
		output.WriteString("parent: ")
		output.WriteString(strconv.Quote(parent))
		output.WriteString("\n")
	}
	if grandParent != "" {
		output.WriteString("grand_parent: ")
		output.WriteString(strconv.Quote(grandParent))
		output.WriteString("\n")
	}
	if navOrder > 0 {
		fmt.Fprintf(&output, "nav_order: %d\n", navOrder)
	}
	if hasChildren {
		output.WriteString("has_children: true\n")
	}
	output.WriteString("permalink: ")
	output.WriteString(permalink)
	output.WriteString("\n---\n\n")

	return output.String()
}

var (
	permalinkPattern     = regexp.MustCompile(`(?m)^permalink:\s*(\S+)\s*$`)
	baseURLPattern       = regexp.MustCompile("\\{\\{\\s*site\\.baseurl\\s*\\}\\}(/[^\\s)\"'<>`]*)")
	markdownLinkPattern  = regexp.MustCompile(`\[[^]]*\]\(([^)[:space:]]+)\)`)
	referenceLinkPattern = regexp.MustCompile(`(?m)^\[[^]]+\]:\s+(\S+)\s*$`)
)

func validateSite(root string) error {
	siteRoot, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer func() { _ = siteRoot.Close() }()
	permalinks, pages, err := collectSitePages(siteRoot, root)
	if err != nil {
		return err
	}
	if err := validateRequiredPages(siteRoot, permalinks); err != nil {
		return err
	}
	for _, path := range pages {
		contents, readErr := siteRoot.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if err := validatePage(siteRoot, path, contents, permalinks); err != nil {
			return err
		}
	}

	return nil
}

func collectSitePages(siteRoot *os.Root, root string) (
	permalinks map[string]string,
	pages []string,
	returnedErr error,
) {
	permalinks = make(map[string]string)
	returnedErr = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}
		relative, relativeErr := filepath.Rel(root, path)
		if relativeErr != nil {
			return relativeErr
		}
		contents, readErr := siteRoot.ReadFile(relative)
		if readErr != nil {
			return readErr
		}
		if !bytes.HasPrefix(contents, []byte("---\n")) || !bytes.Contains(contents, []byte("\ntitle:")) {
			return fmt.Errorf("%s lacks required front matter", path)
		}
		match := permalinkPattern.FindSubmatch(contents)
		if len(match) != 2 {
			return fmt.Errorf("%s lacks a permalink", path)
		}
		permalink := string(match[1])
		if previous, found := permalinks[permalink]; found {
			return fmt.Errorf("duplicate permalink %s in %s and %s", permalink, previous, path)
		}
		permalinks[permalink] = relative
		pages = append(pages, relative)

		return nil
	})
	if returnedErr != nil {
		return nil, nil, returnedErr
	}

	return permalinks, pages, nil
}

func validateRequiredPages(siteRoot *os.Root, permalinks map[string]string) error {
	for _, required := range []string{
		"/", "/getting-started/", "/guides/", "/reference/", "/reference/commands/run/",
		"/operations/", "/project/", "/404.html",
	} {
		if _, found := permalinks[required]; !found {
			return fmt.Errorf("required page %s is missing", required)
		}
	}
	for _, asset := range []string{
		"_config.yml",
		"_includes/head_custom.html",
		"assets/examples/jobman.yml",
		"assets/images/logo.svg",
		"assets/images/logo-dark.svg",
		"assets/images/logo-dark-transparent.svg",
		"assets/images/favicon.svg",
		"assets/images/favicon-dark.svg",
	} {
		if _, err := siteRoot.Stat(asset); err != nil {
			return fmt.Errorf("required asset %s is missing: %w", asset, err)
		}
	}
	for _, name := range demoRecordings {
		asset := filepath.Join("assets", "videos", name)
		if _, err := siteRoot.Stat(asset); err != nil {
			return fmt.Errorf("required asset %s is missing: %w", asset, err)
		}
	}
	themeConfig, err := siteRoot.ReadFile("_config.yml")
	if err != nil {
		return fmt.Errorf("read site theme configuration: %w", err)
	}
	if !bytes.Contains(themeConfig, []byte("remote_theme: just-the-docs/just-the-docs@v")) {
		return errors.New("site remote theme must be pinned to a release")
	}
	headInclude, err := siteRoot.ReadFile("_includes/head_custom.html")
	if err != nil {
		return fmt.Errorf("read site head include: %w", err)
	}
	for _, favicon := range []string{"/assets/images/favicon.svg", "/assets/images/favicon-dark.svg"} {
		if !bytes.Contains(headInclude, []byte(favicon)) {
			return fmt.Errorf("site head include does not reference %s", favicon)
		}
	}

	return nil
}

func validatePage(siteRoot *os.Root, path string, contents []byte, permalinks map[string]string) error {
	for _, forbidden := range []string{"pip install -U jobman", "Python3.9", "Package on PyPI"} {
		if bytes.Contains(contents, []byte(forbidden)) {
			return fmt.Errorf("%s contains legacy documentation text %q", path, forbidden)
		}
	}
	for _, match := range baseURLPattern.FindAllSubmatch(contents, -1) {
		target := strings.SplitN(string(match[1]), "#", 2)[0]
		if strings.HasPrefix(target, "/assets/") {
			relative := filepath.FromSlash(strings.TrimPrefix(target, "/"))
			if _, statErr := siteRoot.Stat(relative); statErr != nil {
				return fmt.Errorf("%s links to missing asset %s", path, target)
			}
			continue
		}
		if _, found := permalinks[target]; !found {
			return fmt.Errorf("%s links to missing page %s", path, target)
		}
	}
	linkMatches := append(markdownLinkPattern.FindAllSubmatch(contents, -1), referenceLinkPattern.FindAllSubmatch(contents, -1)...)
	for _, match := range linkMatches {
		target := string(match[1])
		if strings.HasPrefix(target, "{{") || strings.HasPrefix(target, "#") ||
			strings.HasPrefix(target, "https://") || strings.HasPrefix(target, "mailto:") {
			continue
		}
		return fmt.Errorf("%s contains non-site-relative published link %s", path, target)
	}

	return nil
}

func main() {
	if err := generateSite(".", defaultSiteOutput); err != nil {
		log.Fatal(err)
	}
}
