#!/usr/bin/env python3
import sys

def completion_hook(cmd, curr_word, prev_word):
    potential_matches = ["a", "bunch", "of", "potential", "matches"]
    matches = [k for k in potential_matches if k.startswith(curr_word)]
    return matches

def main():
    results = completion_hook(*sys.argv[1:])
    if len(results):
        print("\n".join(results))

if __name__ == "__main__":
    main()