from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Optional, Union

from typing_extensions import TypedDict

from great_expectations.core import (
    ExpectationValidationResult,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.registry import (
    RendererImpl,
    get_renderer_impl,
    get_renderer_names_with_renderer_types,
)
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
    AtomicRendererType,
    RenderedAtomicContent,
)
from great_expectations.render.exceptions import InlineRendererError
from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.render import RenderedContent

logger = logging.getLogger(__name__)


class InlineRendererConfig(TypedDict):
    class_name: str
    render_object: Union[ExpectationConfiguration, ExpectationValidationResult]


class InlineRenderer(Renderer):
    def __init__(
        self,
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
    ) -> None:
        super().__init__()

        if isinstance(render_object, (ExpectationConfiguration, ExpectationValidationResult)):
            self._render_object = render_object
        else:
            raise InlineRendererError(  # noqa: TRY003
                f"InlineRenderer can only be used with an ExpectationConfiguration or ExpectationValidationResult, but {type(render_object)} was used."  # noqa: E501
            )

    def _get_atomic_rendered_content_for_object(
        self,
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
    ) -> list[RenderedAtomicContent]:
        """Gets RenderedAtomicContent for a given ExpectationConfiguration or ExpectationValidationResult.

        Args:
            render_object: The object to render.

        Returns:
            A list of RenderedAtomicContent objects for a given ExpectationConfiguration or ExpectationValidationResult.
        """  # noqa: E501
        expectation_type: str
        renderer_types: list[AtomicRendererType]
        if isinstance(render_object, ExpectationConfiguration):
            expectation_type = render_object.type
            renderer_types = [AtomicRendererType.PRESCRIPTIVE]
        elif isinstance(render_object, ExpectationValidationResult):
            if render_object.expectation_config:
                expectation_type = render_object.expectation_config.type
            else:
                raise InlineRendererError(  # noqa: TRY003
                    "ExpectationValidationResult passed to InlineRenderer._get_atomic_rendered_content_for_object is missing an expectation_config."  # noqa: E501
                )
            renderer_types = [
                AtomicRendererType.DIAGNOSTIC,
                AtomicRendererType.PRESCRIPTIVE,
            ]
        else:
            raise InlineRendererError(  # noqa: TRY003
                f"InlineRenderer._get_atomic_rendered_content_for_object can only be used with an ExpectationConfiguration or ExpectationValidationResult, but {type(render_object)} was used."  # noqa: E501
            )

        renderer_names: list[
            Union[str, AtomicDiagnosticRendererType, AtomicPrescriptiveRendererType]
        ] = get_renderer_names_with_renderer_types(
            expectation_or_metric_type=expectation_type,
            renderer_types=renderer_types,
        )

        rendered_content: list[RenderedAtomicContent] = (
            self._get_atomic_rendered_content_from_renderer_names(
                render_object=render_object,
                renderer_names=renderer_names,
                expectation_type=expectation_type,
            )
        )

        return rendered_content

    def _get_atomic_rendered_content_from_renderer_names(
        self,
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
        renderer_names: list[
            Union[str, AtomicDiagnosticRendererType, AtomicPrescriptiveRendererType]
        ],
        expectation_type: str,
    ) -> list[RenderedAtomicContent]:
        try_renderer_names: list[
            Union[str, AtomicDiagnosticRendererType, AtomicPrescriptiveRendererType]
        ] = [
            renderer_name
            for renderer_name in renderer_names
            if renderer_name
            not in [
                AtomicPrescriptiveRendererType.FAILED,
                AtomicDiagnosticRendererType.FAILED,
            ]
        ]

        renderer_rendered_content: RenderedAtomicContent
        rendered_content: list[RenderedAtomicContent] = []
        for renderer_name in try_renderer_names:
            renderer_rendered_content = self._get_renderer_atomic_rendered_content(
                render_object=render_object,
                renderer_name=renderer_name,
                expectation_type=expectation_type,
            )
            rendered_content.append(renderer_rendered_content)

        return rendered_content

    @staticmethod
    def _get_renderer_atomic_rendered_content(
        render_object: ExpectationConfiguration | ExpectationValidationResult,
        renderer_name: str | AtomicDiagnosticRendererType | AtomicPrescriptiveRendererType,
        expectation_type: str,
    ) -> RenderedAtomicContent:
        renderer_impl: Optional[RendererImpl]
        try:
            renderer_impl = get_renderer_impl(
                object_name=expectation_type, renderer_type=renderer_name
            )
            if renderer_impl:
                renderer_rendered_content = InlineRenderer._get_rendered_content_from_renderer_impl(
                    renderer_impl=renderer_impl,
                    render_object=render_object,
                )
            else:
                raise InlineRendererError(  # noqa: TRY003, TRY301
                    f"renderer_name: {renderer_name} was not found in the registry for expectation_type: {expectation_type}"  # noqa: E501
                )

            assert isinstance(
                renderer_rendered_content, RenderedAtomicContent
            ), f"The renderer: {renderer_name} for expectation: {expectation_type} should return RenderedAtomicContent."  # noqa: E501
        except Exception as e:
            error_message = f'Renderer "{renderer_name}" failed to render Expectation "{expectation_type} with exception message: {e!s}".'  # noqa: E501
            logger.info(error_message)

            failure_renderer: AtomicPrescriptiveRendererType | AtomicDiagnosticRendererType
            if renderer_name.startswith(AtomicRendererType.PRESCRIPTIVE):
                failure_renderer = AtomicPrescriptiveRendererType.FAILED
                failure_renderer_message = (
                    f'Renderer "{failure_renderer}" will be used to render prescriptive content.'
                )
            else:
                failure_renderer = AtomicDiagnosticRendererType.FAILED
                failure_renderer_message = (
                    f'Renderer "{failure_renderer}" will be used to render diagnostic content.'
                )
            logger.info(failure_renderer_message)

            renderer_impl = get_renderer_impl(
                object_name=expectation_type, renderer_type=failure_renderer
            )
            if renderer_impl:
                renderer_rendered_content = InlineRenderer._get_rendered_content_from_renderer_impl(
                    renderer_impl=renderer_impl,
                    render_object=render_object,
                )
                renderer_rendered_content.exception = error_message
            else:
                raise InlineRendererError(  # noqa: TRY003
                    f'Renderer "{failure_renderer}" was not found in the registry.'
                )

        return renderer_rendered_content

    @staticmethod
    def _get_rendered_content_from_renderer_impl(
        renderer_impl: RendererImpl,
        render_object: ExpectationConfiguration | ExpectationValidationResult,
    ) -> RenderedAtomicContent:
        renderer_fn: Callable[..., RenderedAtomicContent | RenderedContent] = renderer_impl.renderer
        if isinstance(render_object, ExpectationConfiguration):
            renderer_rendered_content = renderer_fn(configuration=render_object)
        else:
            renderer_rendered_content = renderer_fn(result=render_object)
        assert isinstance(renderer_rendered_content, RenderedAtomicContent)
        return renderer_rendered_content

    def get_rendered_content(
        self,
    ) -> list[RenderedAtomicContent]:
        """Gets RenderedAtomicContent for a given object.

        Returns:
            RenderedAtomicContent for a given object.
        """
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult] = (
            self._render_object
        )

        return self._get_atomic_rendered_content_for_object(render_object=render_object)
