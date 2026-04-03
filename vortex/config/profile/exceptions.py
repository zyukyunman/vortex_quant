"""Profile 系统异常定义。"""


class ProfileError(Exception):
    """Profile 基础异常。"""


class ProfileNotFoundError(ProfileError):
    """找不到指定 profile。"""


class ProfileValidationError(ProfileError):
    """Profile 字段或语义校验失败。"""


class ProfileResolutionError(ProfileError):
    """Profile 无法解析为运行时对象。"""