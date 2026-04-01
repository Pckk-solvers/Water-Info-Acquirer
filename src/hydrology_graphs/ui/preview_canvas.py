from __future__ import annotations

import base64
from io import BytesIO

import tkinter as tk
from PIL import Image, ImageTk


def on_preview_area_resized(app, _event=None) -> None:
    """プレビュー領域サイズ変更時に画像を再フィットする。"""

    size = current_preview_canvas_size(app)
    if size is None:
        return
    if app._preview_last_canvas_size == size:
        return
    app._preview_last_canvas_size = size
    if app._preview_image_bytes is None:
        show_preview_placeholder(app, "プレビュー未生成")
        return
    display_preview_image(app, app._preview_image_bytes, force=False)


def display_preview_image(app, image_bytes: bytes, *, force: bool = False) -> None:
    """プレビュー領域に収まるよう縮小して中央表示する。"""

    app._preview_image_bytes = image_bytes
    size = current_preview_canvas_size(app)
    if size is None:
        app.after(50, lambda: display_preview_image(app, image_bytes, force=force))
        return
    max_width, max_height = size
    padding = 12
    max_width = max(1, max_width - padding * 2)
    max_height = max(1, max_height - padding * 2)
    if max_width <= 1 or max_height <= 1:
        app.after(50, lambda: display_preview_image(app, image_bytes, force=force))
        return
    app._preview_last_canvas_size = (max_width, max_height)
    try:
        image = Image.open(BytesIO(image_bytes)).convert("RGBA")
        src_w, src_h = image.size
        if src_w <= 0 or src_h <= 0:
            raise ValueError("invalid image size")
        scale = min(max_width / src_w, max_height / src_h)
        dst_w = max(1, int(src_w * scale))
        dst_h = max(1, int(src_h * scale))
        image_hash = hash(image_bytes)
        if (
            not force
            and app._preview_last_fit_size == (dst_w, dst_h)
            and app._preview_last_image_hash == image_hash
        ):
            return
        if (dst_w, dst_h) != (src_w, src_h):
            image = image.resize((dst_w, dst_h), Image.Resampling.LANCZOS)
        app._preview_photo = ImageTk.PhotoImage(image=image)
        draw_preview_photo(app, app._preview_photo)
        app._preview_last_fit_size = (dst_w, dst_h)
        app._preview_last_image_hash = image_hash
    except Exception:
        encoded = base64.b64encode(image_bytes).decode("ascii")
        app._preview_photo = tk.PhotoImage(data=encoded)
        draw_preview_photo(app, app._preview_photo)
        app._preview_last_fit_size = None
        app._preview_last_image_hash = hash(image_bytes)


def current_preview_canvas_size(app) -> tuple[int, int] | None:
    """プレビューCanvasの有効表示サイズを返す。"""

    canvas = getattr(app, "preview_canvas", None)
    if canvas is None:
        return None
    return (
        max(1, int(canvas.winfo_width()) - 4),
        max(1, int(canvas.winfo_height()) - 4),
    )


def draw_preview_photo(app, photo: tk.PhotoImage) -> None:
    """Canvas中央にプレビュー画像を描画する。"""

    canvas = app.preview_canvas
    width = max(1, int(canvas.winfo_width()))
    height = max(1, int(canvas.winfo_height()))
    cx = width // 2
    cy = height // 2
    if app._preview_placeholder_id is not None:
        canvas.delete(app._preview_placeholder_id)
        app._preview_placeholder_id = None
    if app._preview_canvas_image_id is None:
        app._preview_canvas_image_id = int(canvas.create_image(cx, cy, image=photo, anchor="center"))
    else:
        canvas.coords(app._preview_canvas_image_id, cx, cy)
        canvas.itemconfigure(app._preview_canvas_image_id, image=photo)


def show_preview_placeholder(app, text: str) -> None:
    """画像未表示時のプレースホルダを描画する。"""

    canvas = getattr(app, "preview_canvas", None)
    if canvas is None:
        return
    canvas.delete("all")
    width = max(1, int(canvas.winfo_width()))
    height = max(1, int(canvas.winfo_height()))
    app._preview_canvas_image_id = None
    app._preview_photo = None
    app._preview_placeholder_id = int(
        canvas.create_text(
            width // 2,
            height // 2,
            text=text,
            fill="#E5E7EB",
            anchor="center",
        )
    )
