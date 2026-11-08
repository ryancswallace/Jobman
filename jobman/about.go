package jobman

import (
	"fmt"
	"io"
	"os"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/config"
)

func newAboutCommand(dependencies dependencies, options *rootOptions) *cobra.Command {
	var jsonOutput bool
	command := &cobra.Command{
		Use: "about", Short: "Show configured organization, help links, and message of the day",
		Args: usageArgs(cobra.NoArgs),
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := dependencies.LoadConfig(options)
			if err != nil {
				return err
			}
			if jsonOutput {
				return writeJSON(command, loaded.Config.Presentation)
			}
			return writePresentation(command.OutOrStdout(), loaded.Config.Presentation)
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit presentation configuration as JSON")
	return command
}

func writePresentation(destination io.Writer, presentation config.Presentation) error {
	for _, value := range []string{presentation.DisplayName, presentation.Organization, presentation.DocumentationURL, presentation.SupportURL, presentation.MOTD} {
		if value != "" {
			if _, err := fmt.Fprintln(destination, value); err != nil {
				return fmt.Errorf("write presentation: %w", err)
			}
		}
	}
	return nil
}

//nolint:nilerr // Bare help must remain available when deployment configuration is invalid.
func writeInteractivePresentation(command *cobra.Command, dependencies dependencies, options *rootOptions) error {
	file, ok := command.OutOrStdout().(*os.File)
	if !ok || !isatty.IsTerminal(file.Fd()) {
		return nil
	}
	loaded, err := dependencies.LoadConfig(options)
	// Plain help must remain available even when deployment configuration is broken.
	if err != nil {
		return nil
	}
	return writePresentation(command.OutOrStdout(), loaded.Config.Presentation)
}
