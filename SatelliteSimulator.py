import time
import datetime
import numpy as np
from skyfield.api import load

class SatelliteSimulator:
    def __init__(self, num_satellites=10):
        # Load planetary ephemeris data for Earth-centered calculations
        self.ts = load.timescale()
        self.planets = load('de421.bsp')
        self.earth = self.planets['earth']
        
        self.satellites = []
        for i in range(num_satellites):
            self.add_satellite()

    def add_satellite(self):
        sat = {
            "id": f"SIM-SAT-{len(self.satellites)+1}",
            "payload": np.random.choice(["Comm", "Weather", "Imaging"]),
            "a": np.random.uniform(6878, 8000),  # Semi-major axis (km, Earth's radius + altitude)
            "e": np.random.uniform(0.0, 0.05),   # Eccentricity (0 = circular)
            "i": np.random.uniform(0, 180),      # Inclination (degrees)
            "raan": np.random.uniform(0, 360),   # Right Ascension of Ascending Node
            "argp": np.random.uniform(0, 360),   # Argument of Perigee
            "nu": np.random.uniform(0, 360),     # True Anomaly (starting position)
            "xyzr": None,
            "timestamp": None,
        }
        self.satellites.append(sat)
        return sat

    def crash_satellite(self):
        return self.satellites.pop(np.random.randint(0,len(self.satellites)))

    def compute_position(self, sat, t):
        """Compute satellite position using Keplerian elements."""
        a_km = sat["a"]
        e = sat["e"]
        i = np.radians(sat["i"])
        raan = np.radians(sat["raan"])
        argp = np.radians(sat["argp"])
        nu = np.radians(sat["nu"] + (t % 360))  # Advance orbit over time

        # Convert to Cartesian (simplified approximation)
        r = a_km * (1 - e**2) / (1 + e * np.cos(nu))
        x = r * (np.cos(raan) * np.cos(argp + nu) - np.sin(raan) * np.sin(argp + nu) * np.cos(i))
        y = r * (np.sin(raan) * np.cos(argp + nu) + np.cos(raan) * np.sin(argp + nu) * np.cos(i))
        z = r * (np.sin(i) * np.sin(argp + nu))

        sat["xyzr"] = f"{x}, {y}, {z}, {r}"
        sat["timestamp"] = datetime.datetime.now().strftime('%F %T.%f')#[:-3]

    def run_simulation(self, time_step=0, time_step_interval=10):
        """Run the real-time simulation loop."""
        for sat in self.satellites:
            self.compute_position(sat, time_step)
        time_step += time_step_interval

if __name__ == "__main__":
    simulator = SatelliteSimulator(num_satellites=10)
    time_step = 0
    while True:
        simulator.run_simulation(time_step, time_step_interval=10)
        print(f"{simulator.satellites[0]["id"]} = {simulator.satellites[0]["xyzr"]}")
        time_step += 10
        time.sleep(1)