package jobman

import (
	"context"
	"strings"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/app"
	"github.com/ryancswallace/jobman/internal/model"
)

// diagnoseExtension is intentionally static: completion must not inspect or
// execute arbitrary programs from PATH.
const diagnoseExtension = "diagnose"

func rootArgumentCompletion(
	dependencies dependencies,
	root *rootOptions,
) cobra.CompletionFunc {
	completeJob := jobSelectorCompletion(dependencies, root)

	return func(
		command *cobra.Command,
		arguments []string,
		toComplete string,
	) ([]cobra.Completion, cobra.ShellCompDirective) {
		if root.noExtensions || extensionDisabled(dependencies) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		if len(arguments) == 0 {
			if strings.HasPrefix(diagnoseExtension, toComplete) {
				return []cobra.Completion{
					diagnoseExtension + "\tDiagnose a failed job with the optional companion",
				}, cobra.ShellCompDirectiveNoFileComp
			}

			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		if arguments[0] != diagnoseExtension || strings.HasPrefix(toComplete, "-") {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		parsed, err := parseExtensionArguments(arguments[1:], root)
		if err != nil || parsed.disabled || len(parsed.childArgs) != 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		return completeJob(command, nil, toComplete)
	}
}

func jobSelectorArgumentCompletion(
	dependencies dependencies,
	root *rootOptions,
) cobra.CompletionFunc {
	complete := jobSelectorCompletion(dependencies, root)

	return func(
		command *cobra.Command,
		arguments []string,
		toComplete string,
	) ([]cobra.Completion, cobra.ShellCompDirective) {
		if len(arguments) != 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		return complete(command, arguments, toComplete)
	}
}

func jobSelectorCompletion(dependencies dependencies, root *rootOptions) cobra.CompletionFunc {
	return func(
		command *cobra.Command,
		_ []string,
		toComplete string,
	) ([]cobra.Completion, cobra.ShellCompDirective) {
		if command.Context() == nil {
			command.SetContext(context.Background())
		}
		var completions []cobra.Completion
		err := withBackend(command, dependencies, root, func(backend app.Backend) error {
			jobs, listErr := backend.List(command.Context())
			if listErr != nil {
				return listErr
			}
			completions = matchingJobSelectors(jobs, toComplete)

			return nil
		})
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		return completions, cobra.ShellCompDirectiveNoFileComp
	}
}

func matchingJobSelectors(jobs []model.JobState, prefix string) []cobra.Completion {
	nameCounts := make(map[string]int, len(jobs))
	for _, job := range jobs {
		if name := job.Spec.Name(); name != "" {
			nameCounts[name]++
		}
	}

	completions := make([]cobra.Completion, 0, len(jobs)*2)
	seen := make(map[string]struct{}, len(jobs)*2)
	for _, job := range jobs {
		id := job.ID.String()
		if strings.HasPrefix(id, prefix) {
			if _, exists := seen[id]; !exists {
				completions = append(completions, id)
				seen[id] = struct{}{}
			}
		}
		name := job.Spec.Name()
		if nameCounts[name] != 1 || !strings.HasPrefix(name, prefix) {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		completions = append(completions, name)
		seen[name] = struct{}{}
	}

	return completions
}

func jobSelectorOutcomeCompletion(dependencies dependencies, root *rootOptions) cobra.CompletionFunc {
	complete := jobSelectorCompletion(dependencies, root)

	return func(
		command *cobra.Command,
		arguments []string,
		toComplete string,
	) ([]cobra.Completion, cobra.ShellCompDirective) {
		if strings.Contains(toComplete, "=") {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		completions, directive := complete(command, arguments, toComplete)
		for index := range completions {
			completions[index] += "="
		}

		return completions, directive | cobra.ShellCompDirectiveNoSpace
	}
}

func registerJobSelectorFlagCompletion(
	command *cobra.Command,
	dependencies dependencies,
	root *rootOptions,
	flagNames ...string,
) {
	complete := jobSelectorCompletion(dependencies, root)
	for _, flagName := range flagNames {
		if err := command.RegisterFlagCompletionFunc(flagName, complete); err != nil {
			panic(err)
		}
	}
}
