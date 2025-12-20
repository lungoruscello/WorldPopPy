import matplotlib.pyplot as plt
import runpy
from pathlib import Path

# Config: Map script paths to (output_filename, save_mode)
# Modes:
#   "tight"    -> Trims whitespace
#   "regular"  -> Preserves exact figsize

GALLERY_MAP = {
    "examples/quickstart/01_mekong_population.py":        ("gallery/quick01_mekong_pop.png",    "tight"),
    "examples/quickstart/02_sihanoukville_lights.py":     ("gallery/quick02_sihanoukville.png", "tight"),
    "examples/quickstart/03_korea_lights.py":             ("gallery/quick03_korea.png",         "regular"),
    "examples/quickstart/04_west_africa_growth.py":       ("gallery/quick04_west_africa.png",   "regular"),
    "examples/large_rasters/01_kamchatka_topo_eager.py":  ("gallery/large01_kamchatka.png",     "regular"),
    "examples/large_rasters/02-chile_climate_dask.py":    ("gallery/large02_chile_dask.png",    "regular")
}


def build_gallery():
    # Define root of the repo
    repo_root = Path(__file__).parents[1]
    asset_dir = repo_root / "worldpoppy" / "assets"

    print(f"--- Building Repo Gallery ---")

    for script_rel_path, (img_name, mode) in GALLERY_MAP.items():
        script_path = repo_root / script_rel_path
        output_path = asset_dir / img_name

        if not script_path.exists():
            print(f"⚠️  Skipping missing script: {script_rel_path}")
            continue

        print(f"Running {script_path.name} ({mode} mode)...")

        # 1. Execute the script file
        try:
            # run_name="__lib__" prevents the script's `if __name__ == "__main__":`
            # block from running, so we do not get stuck in plt.show()
            script_globals = runpy.run_path(str(script_path), run_name="__lib__")
        except Exception as e:
            print(f"❌ Error loading {script_path.name}: {e}")
            continue

        # 2. Trigger Plotting Logic
        # Some scripts wrap logic in make_plot(), others run at top-level.
        if "make_plot" in script_globals:
            try:
                script_globals["make_plot"]()
            except Exception as e:
                print(f"❌ Error inside make_plot: {e}")
                continue

        # 3. Handle Saving Logic
        fig = plt.gcf()

        if plt.get_fignums():
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if mode == "tight":
                # Trim whitespace for documentation flow
                fig.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
            else:
                # Respect the strict figsize defined in the script (e.g. 6x6)
                # ensuring perfect alignment in the HTML gallery table.
                fig.savefig(output_path, dpi=300, bbox_inches=None)

            print(f"   ✅ Saved to {img_name}")
            plt.close('all')
        else:
            print(f"   ⚠️ No figure produced by {script_path.name}")


if __name__ == "__main__":
    build_gallery()
