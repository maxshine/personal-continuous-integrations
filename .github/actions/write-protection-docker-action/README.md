## This is an action to run against a GITHUB pull request, force a workflow fail if a pull request attempts to modify a protected file.

### Features
1. Basically, check any specified files get changed by un-privileged users in a pull request.
2. The target files can be specified via a Python styled RegEx in the`include-filter` and
   a fine inclusion can be done via the another `exclude-filter` RegEx to exclude some exceptional files
3. It supports the cross-forks check when `forked-repository-url` specified the forked repository and
   then the action will pull the changes from the forked repository and check it against the action
   hosting repository (where the action is defined and usually the target repository of pull request)
4. Check can be passed for administrative users listed in the parameter `admin-list`. Meanwhile
   to enhance the check security from users add unwanted ones into the list, the action can be
   instructed to get maintain and admin roles collaborators from the host repository. This is activated
   when both `github-access-token` and `github-repository-name` are specified.

### Synopsis in Workflow
The example shows how to use this action to protect files under `.ci` and `.github` directories
The trusted admins will be listed via repository maintain and admin roles
```
- name: Protect File Change By Pull Request
  id: check-protected-files
  uses: <reference to this action via marketplace or repository URL>
  with:
    acting-user: ${{ github.actor }}
    merge-ref: origin/${{ github.event.pull_request.base.ref }}
    head-ref: ${{ github.event.pull_request.head.sha }}
    include-filter: '^ci/.*$|^.github/.*$|^\.pre-commit-config.yaml$'
    github-access-token: ${{ secrets.GITHUB_TOKEN }}
    github-repository-name: ${{ github.repository }}
```

### Inputs
1. `head-ref`: **Required**. The head git reference in a pull request, which can be fetched from `github.event.pull_request.head.sha`. 
2. `merge-ref`: **Required**. The merge target in a pull request, which can be fetched from `github.event.pull_request.base.sha`.
3. `acting-user`: **Required**. The ID of the GitHub user who triggered this pull request, available from `github.actor`.
4. `admin-list`: **Optional**. A semicolon separated GitHub users, who can bypass this check. e.g. `user-a;user-b`.
5. `include-filter`: **Optional**. A regex string to filter which files should be checked. If omitted, NO files are checked
6. `exclude-filter`: **Optional**. A regex string to filter which files should be skipped from the result of `include-filter`
7. `forked-repository-url`: **Optional**. A clone URL for the forked repository URL. When this is set, the tool will do a 
cross-forks check by adding the forked repository via the URL set by this parameter to current host repository and compare
the difference of critical files between the current repository and thr forked repository.
8. `github-access-token`: **Optional**. A GitHub auto token generated for action workflow 
9. `github-repository-name`: **Optional**. The repository full name used along with `github-access-token` to get repository
administrative role collaborators used to be appended with the check's `admin_list`


### Outputs
N/A


### Availability
Available from **v1.0.0**.   
Deprecated from *N/A*  
Please refer to the [Changelog](CHANGELOG.md) for details

### License

**Protect File Change By Pull Request's** license is included here:

```
The MIT License (MIT)

Copyright (c) 2024 Ryan Gao

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```
