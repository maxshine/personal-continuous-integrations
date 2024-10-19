# Change History of [write-protection-docker-action](action.yml)

## Versions
### v1.0.0
- Features
  1. Do the check if the included files are changed in the target pull request. If the author of pull request is an administrator
     then the change is allowed and the check will fail otherwise.
  2. The target files will be all files scoped in this repository by being filtered with the regex of `include-filter` parameter
     and then do another excluding filtering with the regex of `exclude-filter` parameter
  3. The first working version with following parameters:
     1. `head-ref`: Required. The head git reference in a pull request, which can be fetched from `github.event.pull_request.head.sha`. 
     2. `merge-ref`: Required. The merge target in a pull request, which can be fetched from `github.event.pull_request.base.sha`.
     3. `acting-user`: Required. The ID of the GitHub user who triggered this pull request, available from `github.actor`.
     4. `admin-list`: Optional. A semicolon separated GitHub users, who can bypass this check. e.g. `user-a;user-b`.
     5. `include-filter`: Optional. A regex string to filter which files should be checked. If omitted, NO files are checked
     6. `exclude-filter`: Optional. A regex string to filter which files should be skipped from the result of `include-filter`
- Bugfix
N/A

### v1.1.0
- Features
  1. Support GitHub pull requests across forked repositories with a new parameter:
     1. `forked-repository-url`: Optional. A clone URL for the forked repository URL. When this is set, the tool will do a 
        cross-forks check by adding the forked repository via the URL set by this parameter to current host repository and compare
        the difference of critical files between the current repository and thr forked repository.
  2. When the parameter `forked-repository-url` is omitted, then the check will do branches-based check by default

- Bugfix
N/A

### v1.2.0
- Features
  1. Support bypass the write-protection check via collaborator role with new parameters:
     1. `github-access-token`: Optional. A GitHub auto token generated for action workflow 
     2. `github-repository-name`: Optional. The repository full name used along with `github-access-token` to get repository
        administrative role collaborators used to be appended with the check's `admin_list`
  2. When the two parameters of `github-access-token` and `github-repository-name` are set, the check will fetch the 
     collaborators with `admin` role from the target repository and append them after `admin_list`.

- Bugfix
N/A

### v1.2.1 (bugfix release succeeding from v1.2.0)
- Features
N/A

- Bugfix
  1. Fetch `maintain` role collaborators instead of `admin`, which will still include the `admin collaborators

### v1.3.0
- Features
N/A
- Bugfix
N/A

### v1.3.1 (bugfix release succeeding from v1.3.0)
- Features
N/A

- Bugfix
  1. Fetch `maintain` role collaborators instead of `admin`, which will still include the `admin collaborators

### v1.3.2 (bugfix release succeeding from v1.3.1)
- Features
N/A
- Bugfix
N/A