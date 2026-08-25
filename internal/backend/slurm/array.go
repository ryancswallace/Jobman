package slurm

import (
	"errors"
	"fmt"
	"slices"
)

// ArrayTask binds one scheduler index to one independent Jobman execution.
type ArrayTask struct {
	Index       int    `json:"index"`
	ExecutionID string `json:"executionId"`
}

// ArrayPlan is the deterministic result of compiling compatible collection
// children. It contains no command fragments or credentials.
type ArrayPlan struct {
	Tasks       []ArrayTask `json:"tasks"`
	MaxParallel int         `json:"maxParallel"`
}

// CompileArray validates and orders a contiguous set of task bindings.
func CompileArray(tasks []ArrayTask, maxParallel int) (ArrayPlan, error) {
	if len(tasks) < 1 || len(tasks) > 10_000 || maxParallel < 1 || maxParallel > len(tasks) {
		return ArrayPlan{}, errors.New("compile Slurm array: task or concurrency bound is invalid")
	}
	result := ArrayPlan{Tasks: append([]ArrayTask(nil), tasks...), MaxParallel: maxParallel}
	slices.SortFunc(result.Tasks, func(left, right ArrayTask) int { return left.Index - right.Index })
	seen := make(map[string]struct{}, len(result.Tasks))
	for index, task := range result.Tasks {
		if task.Index != index || !validExecutionID(task.ExecutionID) {
			return ArrayPlan{}, fmt.Errorf("compile Slurm array: task %d is invalid or non-contiguous", index)
		}
		if _, exists := seen[task.ExecutionID]; exists {
			return ArrayPlan{}, errors.New("compile Slurm array: execution identity is duplicated")
		}
		seen[task.ExecutionID] = struct{}{}
	}

	return result, nil
}

func validExecutionID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, character := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if character != '-' {
				return false
			}
			continue
		}
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}

	return true
}
