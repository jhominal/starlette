import contextvars
import typing

import anyio

from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

RequestResponseEndpoint = typing.Callable[[Request], typing.Awaitable[Response]]
DispatchFunction = typing.Callable[
    [Request, RequestResponseEndpoint], typing.Awaitable[Response]
]
T = typing.TypeVar("T")


class BaseHTTPMiddleware:
    def __init__(
        self, app: ASGIApp, dispatch: typing.Optional[DispatchFunction] = None
    ) -> None:
        self.app = app
        self.dispatch_func = self.dispatch if dispatch is None else dispatch

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        dispatch_first_phase_ended = anyio.Event()
        streams_ready = anyio.Event()
        request_for_next: typing.Optional[Request] = None
        response_sent = anyio.Event()
        app_exc: typing.Optional[Exception] = None
        dispatch_context_copy: typing.Optional[contextvars.Context] = None

        async def call_next(request: Request) -> Response:
            nonlocal request_for_next, dispatch_context_copy
            request_for_next = request
            dispatch_context_copy = contextvars.copy_context()
            dispatch_first_phase_ended.set()
            await streams_ready.wait()

            try:
                message = await recv_stream.receive()
            except anyio.EndOfStream:
                if app_exc is not None:
                    raise app_exc
                raise RuntimeError("No response returned.")

            assert message["type"] == "http.response.start"

            async def body_stream() -> typing.AsyncGenerator[bytes, None]:
                async with recv_stream:
                    async for message in recv_stream:
                        assert message["type"] == "http.response.body"
                        body = message.get("body", b"")
                        if body:
                            yield body
                        if not message.get("more_body", False):
                            break

                if app_exc is not None:
                    raise app_exc

            response = StreamingResponse(
                status_code=message["status"], content=body_stream()
            )
            response.raw_headers = message["headers"]
            return response

        async def process_dispatch(request: Request):
            nonlocal dispatch_context_copy
            response = await self.dispatch_func(request, call_next)
            await response(scope, receive, send)
            dispatch_context_copy = contextvars.copy_context()
            dispatch_first_phase_ended.set()
            response_sent.set()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(process_dispatch, Request(scope, receive=receive))

            await dispatch_first_phase_ended.wait()

            # Copy contextvars updated from dispatch into the current context.
            for context_var, dispatch_context_value in dispatch_context_copy.items():
                try:
                    if context_var.get() is not dispatch_context_value:
                        context_var.set(dispatch_context_value)
                except LookupError:
                    context_var.set(dispatch_context_value)

            if request_for_next is None:
                return

            async def receive_or_disconnect() -> Message:
                if response_sent.is_set():
                    return {"type": "http.disconnect"}

                async with anyio.create_task_group() as task_group:

                    async def wrap(func: typing.Callable[[], typing.Awaitable[T]]) -> T:
                        result = await func()
                        task_group.cancel_scope.cancel()
                        return result

                    task_group.start_soon(wrap, response_sent.wait)
                    message = await wrap(request_for_next.receive)

                if response_sent.is_set():
                    return {"type": "http.disconnect"}

                return message

            async def close_recv_stream_on_response_sent() -> None:
                await response_sent.wait()
                recv_stream.close()

            async def send_no_error(message: Message) -> None:
                try:
                    await send_stream.send(message)
                except anyio.BrokenResourceError:
                    # recv_stream has been closed, i.e. response_sent has been set.
                    return

            send_stream, recv_stream = anyio.create_memory_object_stream()
            streams_ready.set()

            task_group.start_soon(close_recv_stream_on_response_sent)

            async with send_stream:
                try:
                    await self.app(scope, receive_or_disconnect, send_no_error)
                except Exception as exc:
                    app_exc = exc

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        raise NotImplementedError()  # pragma: no cover
