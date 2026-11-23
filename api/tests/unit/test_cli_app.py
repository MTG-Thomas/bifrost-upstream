import json
import zipfile
from pathlib import Path

from bifrost.commands.app import _scaffold, _zip_project
from bifrost.commands.solution import _v2_scaffold_files


def test_scaffold_is_a_vite_project_with_standard_ignored_env(tmp_path: Path) -> None:
    root = tmp_path / "my-app"
    _scaffold(root, "my-app")

    assert (root / "package.json").is_file()
    assert (root / "vite.config.ts").is_file()
    assert ".env" in (root / ".gitignore").read_text().splitlines()
    readme = (root / "README.md").read_text()
    assert "bifrost app start" in readme
    assert "standalone" not in readme.lower()
    # One starter App.tsx for both scaffolds: the App scaffold ships the
    # Solution scaffold's shell and unwired starter button verbatim.
    app_source = (root / "src" / "App.tsx").read_text()
    assert app_source == _v2_scaffold_files("my-app")["src/App.tsx"]
    assert "link: NavLink" in app_source


def test_foldkit_scaffold_keeps_the_mount_contract_without_react(tmp_path: Path) -> None:
    root = tmp_path / "fk-app"
    _scaffold(root, "fk-app", "foldkit")
    react_root = tmp_path / "react-app"
    _scaffold(react_root, "fk-app")
    package = json.loads((root / "package.json").read_text())
    dependencies = {**package["dependencies"], **package["devDependencies"]}
    assert "foldkit" in dependencies
    assert not {"react", "react-dom", "@vitejs/plugin-react"} & set(dependencies)
    # The platform supplies the SDK at build time, as for React Apps.
    assert "bifrost" not in dependencies

    index_html = (root / "index.html").read_text()
    assert '<meta name="bifrost-app-runtime" content="mount-v1" />' in index_html
    assert 'src="/src/main.ts"' in index_html
    main_ts = (root / "src" / "main.ts").read_text()
    assert "export function mount(mountEl: HTMLElement, bootstrap: BifrostAppBootstrap)" in main_ts
    assert "__BIFROST_APP_MODULES__" in main_ts
    assert 'from "bifrost/core"' in (root / "src" / "bifrost.ts").read_text()

    # Local-dev wiring and theme tokens are shared with the React scaffold.
    vite_config = (root / "vite.config.ts").read_text()
    assert "plugins: [foldkit(), tailwindcss()]" in vite_config
    assert vite_config.replace("foldkit()", "react()").replace(
        'import { foldkit } from "@foldkit/vite-plugin";', 'import react from "@vitejs/plugin-react";'
    ).replace("main.ts ", "main.tsx ") == (react_root / "vite.config.ts").read_text()
    assert (root / "src" / "index.css").read_text() == (react_root / "src" / "index.css").read_text()
    # TypeScript 7 rejects baseUrl.
    assert "baseUrl" not in json.loads((root / "tsconfig.json").read_text())["compilerOptions"]
    assert ".env" in (root / ".gitignore").read_text().splitlines()
    assert not (root / "src" / "main.tsx").exists()


def test_deploy_archive_never_contains_env_or_generated_files(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.tsx").write_text("export {}")
    (tmp_path / "package.json").write_text("{}")
    (tmp_path / "index.html").write_text("<div id='root'>")
    (tmp_path / ".env").write_text("SECRET=do-not-upload")
    (tmp_path / "node_modules" / "x").mkdir(parents=True)
    (tmp_path / "node_modules" / "x" / "index.js").write_text("generated")
    destination = tmp_path.parent / "source.zip"

    _zip_project(tmp_path, destination)

    with zipfile.ZipFile(destination) as archive:
        names = set(archive.namelist())
    assert "src/main.tsx" in names
    assert ".env" not in names
    assert not any(name.startswith("node_modules/") for name in names)
