"""Repository 抽象接口定义。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class User:
    """用户持久化模型。"""

    id: int | None = None
    username: str = ""
    api_key: str = ""
    role: str = "user"              # "admin" | "user"
    namespace: str = ""
    disabled: bool = False
    max_connections: int = 10
    created_at: float = 0.0
    updated_at: float = 0.0


@dataclass
class PermissionGroup:
    """权限组。"""

    id: int | None = None
    name: str = ""
    created_at: float = 0.0


@dataclass
class GroupPermission:
    """组与权限规则的关联。"""

    id: int | None = None
    group_id: int = 0
    topic_pattern: str = ""         # "*.mkt.*"
    action: str = ""                # "pub" | "sub" | "query"


class UserRepository(ABC):
    """用户数据访问接口。"""

    @abstractmethod
    async def get_by_id(self, user_id: int) -> User | None: ...

    @abstractmethod
    async def get_by_api_key(self, api_key: str) -> User | None: ...

    @abstractmethod
    async def create(self, user: User) -> User: ...

    @abstractmethod
    async def update(self, user: User) -> User: ...

    @abstractmethod
    async def delete(self, user_id: int) -> None: ...

    @abstractmethod
    async def list_all(self) -> list[User]: ...


class PermissionGroupRepo(ABC):
    """权限组数据访问接口。"""

    # 权限组 CRUD
    @abstractmethod
    async def create_group(self, name: str) -> PermissionGroup: ...

    @abstractmethod
    async def delete_group(self, group_id: int) -> None: ...

    @abstractmethod
    async def get_group(self, group_id: int) -> PermissionGroup | None: ...

    @abstractmethod
    async def list_groups(self) -> list[PermissionGroup]: ...

    # 权限管理
    @abstractmethod
    async def add_permission(self, group_id: int, topic_pattern: str, action: str) -> None: ...

    @abstractmethod
    async def remove_permission(self, group_id: int, topic_pattern: str, action: str) -> None: ...

    @abstractmethod
    async def get_permissions(self, group_id: int) -> list[GroupPermission]: ...

    # 成员管理
    @abstractmethod
    async def add_member(self, group_id: int, user_id: int) -> None: ...

    @abstractmethod
    async def remove_member(self, group_id: int, user_id: int) -> None: ...

    @abstractmethod
    async def get_members(self, group_id: int) -> list[User]: ...

    @abstractmethod
    async def get_user_groups(self, user_id: int) -> list[PermissionGroup]: ...

    # 权限展开查询
    @abstractmethod
    async def get_user_expanded_permissions(self, user_id: int) -> dict[str, list[str]]: ...

    @abstractmethod
    async def get_group_all_members(self, group_id: int) -> list[int]: ...
