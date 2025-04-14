import os
import json
import time
from collections import deque

import keyboard
import numpy as np
from confluent_kafka import Producer
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.text import Text
from rich.markup import escape
from rich.syntax import Syntax

from SatelliteSimulator import SatelliteSimulator

simulator = None
keep_alive = True
pause = False
system_messages = []
current_system_message = 1
current_index = 0

last_payload = ""
last_key = ""
latencies = deque(maxlen=100)
message_count = 0
time_step = 0
time_step_increment = 2

load_dotenv()

conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD'),
}
topic = os.getenv('TOPIC')

producer = Producer(conf)
console = Console()

splash = r"""
_________             ______        _____________         
______  /_____ __________  /_______ ___  __/__  /_______ _
_  __  /_  __ `/_  __ \_  //_/  __ `/_  /_ __  //_/  __ `/
/ /_/ / / /_/ /_  / / /  ,<  / /_/ /_  __/ _  ,<  / /_/ / 
\__,_/  \__,_/ /_/ /_//_/|_| \__,_/ /_/    /_/|_| \__,_/  
              [Kafka Producer Dashboard] v1.0
"""
console.clear()
console.print(splash, style="bold magenta")

console.print("[green][✔] Secure connection established with Kafka endpoint[/green]")
console.print(f"[dim]Topic:[/] [bold cyan]{topic}[/bold cyan]")
console.print()

def add_system_message(message):
    global current_system_message, current_index
    system_messages.append(message) 
    current_index = len(system_messages)

def key_press(event):
    global keep_alive, system_messages, current_system_message, current_index, pause, simulator
    if event.name == "esc":
        keep_alive = not keep_alive
    elif event.name == "space":
        pause = not pause
        system_messages.append(f"hit { len(system_messages)} {pause}")
    elif event.name == "n":
        current_index = (current_index + 1) % len(system_messages) 
    elif event.name == "p":
        current_index = (current_index - 1) % len(system_messages)
    elif event.name == "l":
        sat = simulator.add_satellite()
        system_messages.append(f"Satellite {sat["id"]} [green]launched![/green]")
        current_index = (current_index + 1) % len(system_messages) 
    elif event.name == "c":
        sat = simulator.crash_satellite()
        system_messages.append(f"Satellite {sat["id"]} [red]crashed![/red]")
        current_index = (current_index - 1) % len(system_messages)  

def format_latency_color(latency_ms):
    if latency_ms < 100:
        return f"[green]{latency_ms:.2f} ms[/green]"
    elif latency_ms < 300:
        return f"[yellow]{latency_ms:.2f} ms[/yellow]"
    else:
        return f"[red]{latency_ms:.2f} ms[/red]"

def make_sparkline(data, width=20):
    # Normalize to 0–8 (because sparkline blocks have 8 levels)
    blocks = '▁▂▃▄▅▆▇█'
    if not data:
        return ''
    max_val = max(data)
    min_val = min(data)
    span = max_val - min_val or 1e-6  # prevent div by 0
    scaled = [(val - min_val) / span * (len(blocks) - 1) for val in data]
    return ''.join(blocks[int(val)] for val in scaled)

def build_dashboard():
    global latencies
    table = Table.grid()
    table.add_row("[bold cyan]Orbital Simulator Dashboard[/bold cyan]")
    table.add_row("─" * 90)
    table.add_row(f"[green]Messages Sent:[/green] {message_count}")

    avg_latency = (sum(latencies) / len(latencies)) * 1000 if latencies else 0
    latency_ms = avg_latency
    latency_display = format_latency_color(latency_ms)

    spark_data = [l * 1000 for l in list(latencies)[-60:]]
    sparkline = make_sparkline(spark_data)

    if len(latencies) > 10:
        table.add_row(f"[yellow]Avg Delivery Latency:[/yellow] {latency_display} [dim](high of [bold red]{(1000*max(latencies)):.2f}[/bold red], low of [bold green]{(1000*min(latencies)):.2f}[/bold green], over last [bold white]{len(latencies)}[/bold white] samples)[/dim]")
    else:
        table.add_row(f"[yellow]Avg Delivery Latency:[/yellow] {latency_display}")
    table.add_row(f"[magenta]Latency Sparkline:[/magenta] {sparkline or '[dim]n/a[/dim]'}")

    if last_key:
        table.add_row(f"[blue]Last Key:[/blue] {last_key}")
    table.add_row("─" * 90)
    table.add_row("[bold]Last Payload:[/bold]")
    try:
        parsed = json.loads(last_payload)
        json_pretty = json.dumps(parsed, indent=2)
        syntax = Syntax(json_pretty, "json", theme="monokai", word_wrap=True)
        table.add_row(syntax)
    except:
        table.add_row(Text(last_payload, style="dim"))
    table.add_row("─" * 90)
    sm = ""
    if len(system_messages)>1:
        sm = system_messages[current_index-1]

    table.add_row(f"[bold white]System Message[/bold white] [white]({current_index+1} of {len(system_messages)}):\t{sm}")
    table.add_row("─" * 90)
    table.add_row("[bold cyan][L][/bold cyan][dim]aunch or[/dim] [bold cyan][C][/bold cyan][dim]rash satellite[/dim]\t[bold cyan][N][/bold cyan][dim]ext or [/dim][bold cyan][P][/bold cyan][dim]revious message[/dim]\t[bold cyan][Space][/bold cyan] [dim]pause[/dim]\t[bold cyan][Esc][/bold cyan] [dim]quit[/dim]")
    return Panel(table, border_style="blue")

def delivery_report(err, msg):
    global last_payload, last_key, message_count, latencies
    payload = msg.value().decode("utf-8")
    last_payload = payload
    last_key = msg.key().decode("utf-8") if msg.key() else ""
    message_count += 1
    if err is not None:
        latencies.append(0)
    else:
        latencies.append(msg.latency())

def main():
    global simulator, time_step, time_step_increment
    add_system_message("Program started...")
    try:
        keyboard.on_release(key_press)
        simulator = SatelliteSimulator(num_satellites=10)
        with Live(build_dashboard(), refresh_per_second=5, console=console) as live:
            add_system_message("Services initialized...")
            last_time = time.perf_counter()
            while keep_alive:
                current_time = time.perf_counter()
                if current_time - last_time >= 1:  # Run every second
                    live.update(build_dashboard())
                    last_time = current_time

                    simulator.run_simulation(time_step, time_step_increment)
                    for sat in simulator.satellites: 
                        payload_json = json.dumps(sat)
                        key = sat["id"] 
                        ts = int(time.time() * 1000)
                        producer.produce(
                            topic=topic,
                            key=key.encode("utf-8"),
                            value=payload_json,
                            timestamp=ts,
                            callback=delivery_report
                        )
                        producer.poll(0)
                    time_step += time_step_increment
                time.sleep(0.01)
                producer.flush(0)
    except KeyboardInterrupt:
        console.print("\n[bold red]Program interrupted. Cleaning up...[/bold red]")
    finally: 
        producer.flush()
        console.print("\n[green]Clean exit.[/green]\n")

if __name__ == "__main__":
    main()