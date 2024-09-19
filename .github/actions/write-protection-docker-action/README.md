## This is an action to run against a GITHUB pull request, checking any protected files changed

### Inputs
1. `head-ref`: Required. The head git reference in a pull request, which can be fetched from `github.event.pull_request.head.sha`. 
2. `merge-ref`: Required. The merge target in a pull request, which can be fetched from `github.event.pull_request.base.sha`.
3. `acting-user`: Required. The ID of the Github user who triggered this pull request, available from `github.actor`.
4. `admin-list`: Optional. A semicolon separated Github users, who can bypass this check. e.g. `user-a;user-b`.
5. `include-filter`: Optional. A regex string to filter which files should be checked. If omitted, NO files are checked
6. `exclude-filter`: Optional. A regex string to filter which files should be skipped from the result of `include-filter`
7. `forked-repository-url`: Optional. A clone URL for the forked repository URL. When this is set, the tool will do a 
cross-forks check by adding the forked repository via the URL set by this parameter to current host repository and compare
the difference of critical files between the current repository and thr forked repository.
8. `github-access-token`: Optional. A Github auto token generated for action workflow 
9. `github-repository-name`: Optional. The repository full name used along with `github-access-token` to get repository
administrative role collaborators used to be appended with the check's `admin_list`


### Outputs
N/A