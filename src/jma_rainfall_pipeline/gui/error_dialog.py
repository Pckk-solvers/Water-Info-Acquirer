import tkinter as tk
from tkinter import ttk, messagebox
import traceback
import pyperclip

class ErrorDialog(tk.Toplevel):
    def __init__(self, parent, title, message, exception=None):
        super().__init__(parent)
        self.title(f"エラー: {title}")
        self.geometry("600x400")
        self.resizable(True, True)
        
        # メインフレーム
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # エラーメッセージ
        msg_label = ttk.Label(
            main_frame, 
            text=message,
            wraplength=550,
            justify=tk.LEFT
        )
        msg_label.pack(fill=tk.X, pady=(0, 10))
        
        # エラーの詳細（例外がある場合）
        if exception:
            self.error_text = f"{message}\n\n{traceback.format_exc()}"
            
            # エラー詳細ラベル
            ttk.Label(
                main_frame, 
                text="エラーの詳細:",
                font=('TkDefaultFont', 9, 'bold')
            ).pack(anchor=tk.W)
            
            # テキストボックス
            text_frame = ttk.Frame(main_frame)
            text_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
            
            self.text = tk.Text(
                text_frame,
                wrap=tk.WORD,
                font=('Courier', 9),
                bg='#f0f0f0',
                padx=5,
                pady=5
            )
            self.text.insert(tk.END, self.error_text)
            self.text.config(state=tk.DISABLED)
            
            # スクロールバー
            scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.text.yview)
            self.text.configure(yscrollcommand=scrollbar.set)
            
            # グリッド配置
            self.text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            # ボタンフレーム
            btn_frame = ttk.Frame(main_frame)
            btn_frame.pack(fill=tk.X, pady=(5, 0))
            
            # コピーボタン
            copy_btn = ttk.Button(
                btn_frame,
                text="エラーをクリップボードにコピー",
                command=self.copy_to_clipboard
            )
            copy_btn.pack(side=tk.LEFT, padx=(0, 10))
            
            # 閉じるボタン
            close_btn = ttk.Button(
                btn_frame,
                text="閉じる",
                command=self.destroy
            )
            close_btn.pack(side=tk.LEFT)
        else:
            # 例外がない場合の閉じるボタン
            close_btn = ttk.Button(
                main_frame,
                text="閉じる",
                command=self.destroy
            )
            close_btn.pack(pady=(10, 0))
        
        # 中央に配置
        self.update_idletasks()
        self.geometry(f"+{parent.winfo_x() + parent.winfo_width()//2 - self.winfo_width()//2}+"
                     f"{parent.winfo_y() + parent.winfo_height()//2 - self.winfo_height()//2}")
    
    def copy_to_clipboard(self):
        """エラーメッセージをクリップボードにコピー"""
        try:
            pyperclip.copy(self.error_text)
            messagebox.showinfo("コピー完了", "エラーメッセージをクリップボードにコピーしました。")
        except Exception as e:
            messagebox.showerror("エラー", f"クリップボードへのコピーに失敗しました: {str(e)}")

def show_error(parent, title, message, exception=None):
    """エラーダイアログを表示するヘルパー関数"""
    dialog = ErrorDialog(parent, title, message, exception)
    dialog.transient(parent)
    dialog.grab_set()
    return dialog
