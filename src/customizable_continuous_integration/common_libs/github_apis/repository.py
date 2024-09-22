"""The Github API for repositories

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  17/09/2024   Ryan, Gao       Initial creation
"""

from github import Auth, Github


def get_repository_by_permission(github_access_token: str, repository_name: str, permission: str = None) -> list[str]:
    auth = Auth.Token(github_access_token)
    print(github_access_token)
    r = Github(auth=auth).get_repo(repository_name)
    if permission:
        admin_paged_list = r.get_collaborators(permission=permission, affiliation="all")
    else:
        admin_paged_list = r.get_collaborators()
    ret = []
    for i in range(admin_paged_list.totalCount):
        ret.extend([n.login for n in admin_paged_list.get_page(i)])
    return ret
