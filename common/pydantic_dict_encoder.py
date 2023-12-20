from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    ForwardRef,
    Optional,
    Type,
    Union,
    cast,
)

from pydantic import BaseConfig, BaseModel

if TYPE_CHECKING:
    from pydantic.typing import (
        AbstractSetIntStr,
        AnyCallable,
        MappingIntStrAny,
    )

__all__ = ("PydanticDictEncodersMixin",)


class PydanticDictEncodersMixin:
    """
    Pydantic mixin for support dict encode like json (json_encoders/json).
    ```
    class AnyModel(PydanticDictEncodersMixin, BaseModel):
        any_field: str | None = None

        class Config:
            dict_encoders = {}
            jsonify_dict_encode = False
    ```

    WARNING! Please, remember about python MRO: BaseModel MUST BE after mixin.

    Config specification:
        - `dict_encoders` - map of dict encoders. Working as standart pydantic `json_encoders`
        - `jsonify_dict_encode` - flag of convert values as though using .json() for each fields
    """  # noqa: E501

    class Config(BaseConfig):
        # Dict encoders like json_encoders
        dict_encoders: Dict[Union[Type[Any], str, ForwardRef], "AnyCallable"] = {}
        # To dict encoding value like json value
        jsonify_dict_encode: bool = False

    if TYPE_CHECKING:
        __config__: ClassVar[Type["Config"]] = Config
        __json_encoder__: ClassVar[Callable[[Any], Any]]

    @classmethod
    def _get_value_like_json(
        cls,
        v: Any,
    ) -> Any:
        v = cls.__config__.json_dumps(v, default=cls.__json_encoder__)
        if v.startswith('"'):
            return v[1:-1]
        return v

    @classmethod
    def _get_value_custom_encoder(cls, v: Any) -> Optional[Callable[[Any], Any]]:
        for _type, encoder in cls.__config__.dict_encoders.items():
            if isinstance(v, _type):  # type: ignore
                return encoder
        return None

    @classmethod
    def _get_value(
        cls,
        v: Any,
        to_dict: bool,
        by_alias: bool,
        include: Optional[Union["AbstractSetIntStr", "MappingIntStrAny"]],
        exclude: Optional[Union["AbstractSetIntStr", "MappingIntStrAny"]],
        exclude_unset: bool,
        exclude_defaults: bool,
        exclude_none: bool,
    ) -> Any:
        encoder = None
        dict_encoders = getattr(cls.__config__, "dict_encoders", {})
        jsonify_dict_encode = getattr(cls.__config__, "jsonify_dict_encode", False)
        if dict_encoders:
            encoder = cast(Callable[[Any], Any], cls._get_value_custom_encoder(v))
        if (
            not encoder
            and jsonify_dict_encode
            and not isinstance(v, (list, dict, BaseModel))
        ):
            encoder = cls._get_value_like_json
        if jsonify_dict_encode and isinstance(v, (PydanticDictEncodersMixin)):
            v.__config__.jsonify_dict_encode = True
        if encoder:
            v = encoder(v)
        return super()._get_value(  # type: ignore
            v,
            to_dict,
            by_alias,
            include,
            exclude,
            exclude_unset,
            exclude_defaults,
            exclude_none,
        )
