import matplotlib.pyplot as plt
import runpy
from pathlib import Path

# Config: Map script paths to output filenames
GALLERY_MAP = {
    "examples/quickstart/01_mekong_population.py":    "gallery/quick01_mekong_pop.png",
    "examples/quickstart/02_sihanoukville_lights.py": "gallery/quick02_sihanoukville.png",
    "examples/quickstart/03_korea_lights.py":         "gallery/quick03_korea.png",
    "examples/quickstart/04_west_africa_growth.py":   "gallery/quick04_west_africa.png",
    "examples/large_rasters/01_kamchatka_topo_eager.py":  "gallery/large01_kamchatka.png",
    "examples/large_rasters/02-chile_weather_dask.py":    "gallery/large02_chile_dask.png",
}


def build_gallery():
    # Define root of the repo
    repo_root = Path(__file__).parents[1]
    asset_dir = repo_root / "worldpoppy" / "assets"

    print(f"--- Building Repo Gallery ---")

    for script_rel_path, img_name in GALLERY_MAP.items():
        script_path = repo_root / script_rel_path
        output_path = asset_dir / img_name

        if not script_path.exists():
            print(f"⚠️  Skipping missing script: {script_rel_path}")
            continue

        print(f"Running {script_path.name}...")

        # 1. Execute the script file to load definitions
        try:
            # We use "__lib__" so the script knows it is NOT being run as main
            script_globals = runpy.run_path(str(script_path), run_name="__lib__")
        except Exception as e:
            print(f"❌ Error loading {script_path.name}: {e}")
            continue

        # 2. Check for 'make_plot' function (Dask/Safe Pattern)
        if "make_plot" in script_globals:
            print("   ⚙️ Found 'make_plot' function. Executing...")
            try:
                # This runs the logic (and safely creates/destroys the Dask Client)
                script_globals["make_plot"]()
            except Exception as e:
                print(f"❌ Error inside make_plot: {e}")
                continue
        else:
            # 3. Fallback: Assume script ran logic at top-level
            print("   ℹ️ No 'make_plot' found. Using top-level execution results.")

        # 4. Grab the current figure
        fig = plt.gcf()

        # Check if a figure actually exists (handle scripts that might fail silently)
        if plt.get_fignums():
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300)
            print(f"   ✅ Saved to {img_name}")
            plt.close('all')
        else:
            print(f"   ⚠️ No figure produced by {script_path.name}")


if __name__ == "__main__":
    build_gallery()
