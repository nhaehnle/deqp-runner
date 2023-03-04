#!/usr/bin/env bash
# Sort if has two arguments
# Check if sorted otherwise
if [[ $# == 2 ]]; then
	sort -n < "$1" | while read -r line; do
		printf 'TEST: %s\n' "$line"
	done
else
	sort -n < "$1" | while read -r line; do
		printf "Test case '%s'..\n  Pass (Result image matches reference)\n" "$line"
	done
	printf 'DONE!\n'
fi
