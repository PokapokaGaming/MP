#!/usr/bin/env python3
"""
Dynamic Counter Display - GUI for MPFRP Demo
各PCで実行してカウンター値を大きく表示
"""

import tkinter as tk
from tkinter import ttk, font
import socket
import threading
import argparse
import struct
import time

class CounterDisplay:
    def __init__(self, node_name="client1", server_host="localhost", server_port=9999):
        self.node_name = node_name
        self.server_host = server_host
        self.server_port = server_port
        self.counter_value = 0
        self.connected = False
        
        self.setup_gui()
        self.setup_network()
    
    def setup_gui(self):
        self.root = tk.Tk()
        self.root.title(f"MPFRP Counter - {self.node_name}")
        self.root.geometry("800x600")
        self.root.configure(bg='#1a1a2e')
        
        # メインフレーム
        main_frame = tk.Frame(self.root, bg='#1a1a2e')
        main_frame.pack(expand=True, fill='both', padx=20, pady=20)
        
        # タイトル
        title_font = font.Font(family="Helvetica", size=24, weight="bold")
        title_label = tk.Label(
            main_frame, 
            text=f"Node: {self.node_name}",
            font=title_font,
            fg='#eee',
            bg='#1a1a2e'
        )
        title_label.pack(pady=10)
        
        # カウンター表示（大きな数字）
        counter_font = font.Font(family="Helvetica", size=200, weight="bold")
        self.counter_label = tk.Label(
            main_frame,
            text="0",
            font=counter_font,
            fg='#00ff88',
            bg='#1a1a2e'
        )
        self.counter_label.pack(expand=True)
        
        # ボタンフレーム
        button_frame = tk.Frame(main_frame, bg='#1a1a2e')
        button_frame.pack(pady=20)
        
        # INCREMENTボタン
        button_font = font.Font(family="Helvetica", size=24, weight="bold")
        self.increment_button = tk.Button(
            button_frame,
            text="INCREMENT",
            font=button_font,
            bg='#4a90d9',
            fg='white',
            activebackground='#6ba3e0',
            padx=40,
            pady=20,
            command=self.on_increment
        )
        self.increment_button.pack(side='left', padx=10)
        
        # 接続状態
        self.status_var = tk.StringVar(value="Disconnected")
        status_font = font.Font(family="Helvetica", size=14)
        self.status_label = tk.Label(
            main_frame,
            textvariable=self.status_var,
            font=status_font,
            fg='#ff6b6b',
            bg='#1a1a2e'
        )
        self.status_label.pack(pady=10)
        
        # キーボードショートカット
        self.root.bind('<space>', lambda e: self.on_increment())
        self.root.bind('<Return>', lambda e: self.on_increment())
        self.root.bind('<q>', lambda e: self.root.quit())
    
    def setup_network(self):
        """ネットワーク接続のセットアップ（オプション）"""
        # 実際の分散環境では、ここでErlangノードとの通信を設定
        # このプロトタイプでは、ローカルのErlangシェルと標準入出力で通信
        self.status_var.set("Local Mode")
        self.status_label.config(fg='#00ff88')
    
    def on_increment(self):
        """ボタン押下時の処理"""
        print(f"INCREMENT:{self.node_name}")  # Erlang側で読み取る
        # ローカルでの視覚フィードバック
        self.increment_button.config(bg='#6ba3e0')
        self.root.after(100, lambda: self.increment_button.config(bg='#4a90d9'))
    
    def update_counter(self, value):
        """カウンター値を更新"""
        self.counter_value = value
        self.counter_label.config(text=str(value))
        # 更新時のアニメーション
        self.counter_label.config(fg='#ffff00')
        self.root.after(200, lambda: self.counter_label.config(fg='#00ff88'))
    
    def listen_stdin(self):
        """標準入力からカウンター更新を受け取る"""
        import sys
        while True:
            try:
                line = sys.stdin.readline().strip()
                if line.startswith("COUNTER:"):
                    value = int(line.split(":")[1])
                    self.root.after(0, lambda v=value: self.update_counter(v))
            except:
                pass
    
    def run(self):
        # 標準入力リスナー開始
        listener = threading.Thread(target=self.listen_stdin, daemon=True)
        listener.start()
        
        # GUIメインループ
        self.root.mainloop()


class SimpleDisplay:
    """シンプルなターミナル表示（GUIなし）"""
    
    def __init__(self, node_name="client1"):
        self.node_name = node_name
        self.counter_value = 0
    
    def update_counter(self, value):
        self.counter_value = value
        self.display()
    
    def display(self):
        # ターミナルをクリアして大きく表示
        print("\033[2J\033[H")  # Clear screen
        print("=" * 50)
        print(f"  Node: {self.node_name}")
        print("=" * 50)
        print()
        print(f"       {self.counter_value:^10}")
        print()
        print("=" * 50)
        print("Press Enter to increment, 'q' to quit")
    
    def run(self):
        self.display()
        while True:
            try:
                cmd = input()
                if cmd.lower() == 'q':
                    break
                elif cmd == '':
                    print(f"INCREMENT:{self.node_name}")
            except EOFError:
                break


def main():
    parser = argparse.ArgumentParser(description='MPFRP Counter Display')
    parser.add_argument('--node', '-n', default='client1', help='Node name')
    parser.add_argument('--host', '-H', default='localhost', help='Server host')
    parser.add_argument('--port', '-p', type=int, default=9999, help='Server port')
    parser.add_argument('--simple', '-s', action='store_true', help='Use simple terminal display')
    args = parser.parse_args()
    
    if args.simple:
        display = SimpleDisplay(args.node)
    else:
        display = CounterDisplay(args.node, args.host, args.port)
    
    display.run()


if __name__ == "__main__":
    main()
