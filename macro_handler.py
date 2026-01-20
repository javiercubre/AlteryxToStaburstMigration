"""
Macro handler with interactive prompts for missing macros.
"""
import os
from pathlib import Path
from typing import Optional, List, Dict, Set
from dataclasses import dataclass, field

from models import AlteryxWorkflow, MacroInfo
from alteryx_parser import AlteryxParser


@dataclass
class MacroResolver:
    """Handles macro resolution with caching and interactive prompts."""

    # Directories to search for macros
    search_directories: List[Path] = field(default_factory=list)

    # Cache of resolved macro paths
    resolved_paths: Dict[str, str] = field(default_factory=dict)

    # Macros to skip
    skip_macros: Set[str] = field(default_factory=set)

    # Skip all missing macros (non-interactive mode)
    skip_all: bool = False

    # Interactive mode flag
    interactive: bool = True

    def __post_init__(self):
        # Ensure search_directories is a list of Path objects
        self.search_directories = [Path(d) for d in self.search_directories]

    def add_search_directory(self, directory: str) -> None:
        """Add a directory to search for macros."""
        path = Path(directory)
        if path.exists() and path.is_dir():
            if path not in self.search_directories:
                self.search_directories.append(path)
                print(f"Added macro search directory: {path}")
        else:
            print(f"Warning: Directory does not exist: {directory}")

    def resolve_macros(self, workflow: AlteryxWorkflow) -> Dict[str, MacroInfo]:
        """Resolve all macros in a workflow."""
        macro_infos = {}

        for macro_path in workflow.macros_used:
            if macro_path in self.skip_macros:
                workflow.missing_macros.append(macro_path)
                continue

            macro_info = self._resolve_macro(macro_path, workflow)
            macro_infos[macro_path] = macro_info

            if not macro_info.found:
                workflow.missing_macros.append(macro_path)

        return macro_infos

    def _resolve_macro(self, macro_ref: str, workflow: AlteryxWorkflow) -> MacroInfo:
        """Resolve a single macro reference."""
        macro_name = Path(macro_ref).name

        macro_info = MacroInfo(
            name=macro_name,
            file_path=macro_ref,
        )

        # Check if already resolved
        if macro_ref in self.resolved_paths:
            resolved = self.resolved_paths[macro_ref]
            if Path(resolved).exists():
                macro_info.found = True
                macro_info.resolved_path = resolved
                macro_info.workflow = self._parse_macro(resolved)
                return macro_info

        # Try to find the macro
        resolved_path = self._search_for_macro(macro_ref, workflow)

        if resolved_path:
            macro_info.found = True
            macro_info.resolved_path = str(resolved_path)
            self.resolved_paths[macro_ref] = str(resolved_path)

            # Parse the macro
            macro_info.workflow = self._parse_macro(str(resolved_path))

            # Extract inputs/outputs
            if macro_info.workflow:
                for node in macro_info.workflow.nodes:
                    if node.plugin_name == "Macro Input":
                        macro_info.inputs.append(node.annotation or f"Input_{node.tool_id}")
                    elif node.plugin_name == "Macro Output":
                        macro_info.outputs.append(node.annotation or f"Output_{node.tool_id}")

        elif not self.skip_all and self.interactive:
            # Prompt user for macro location
            macro_info = self._prompt_for_macro(macro_ref, workflow, macro_info)

        return macro_info

    def _search_for_macro(self, macro_ref: str, workflow: AlteryxWorkflow) -> Optional[Path]:
        """Search for a macro in multiple locations."""
        macro_name = Path(macro_ref).name
        workflow_dir = Path(workflow.metadata.file_path).parent

        # Search locations in priority order
        search_locations = [
            # 1. Exact path from workflow
            Path(macro_ref),

            # 2. Relative to workflow file
            workflow_dir / macro_ref,
            workflow_dir / macro_name,

            # 3. macros/ subdirectory relative to workflow
            workflow_dir / "macros" / macro_name,
            workflow_dir / "Macros" / macro_name,

            # 4. Parent directory
            workflow_dir.parent / macro_name,
            workflow_dir.parent / "macros" / macro_name,
        ]

        # Add search directories
        for search_dir in self.search_directories:
            search_locations.append(search_dir / macro_name)
            # Also search subdirectories
            for subdir in search_dir.glob("**/"):
                search_locations.append(subdir / macro_name)

        # Try each location
        for location in search_locations:
            if location.exists() and location.is_file():
                return location.resolve()

        return None

    def _prompt_for_macro(self, macro_ref: str, workflow: AlteryxWorkflow,
                          macro_info: MacroInfo) -> MacroInfo:
        """Interactively prompt user for macro location."""
        macro_name = Path(macro_ref).name

        print("\n" + "=" * 60)
        print(f"Macro not found: \"{macro_name}\"")
        print(f"Referenced in: {workflow.metadata.name}")
        if macro_ref != macro_name:
            print(f"Original path: {macro_ref}")
        print("=" * 60)
        print("\nOptions:")
        print("[1] Enter path to macro file")
        print("[2] Enter directory containing macros")
        print("[3] Skip this macro (document as missing)")
        print("[4] Skip all missing macros")
        print()

        while True:
            try:
                choice = input("Your choice (1-4): ").strip()

                if choice == "1":
                    path = input("Enter full path to macro file: ").strip()
                    path = path.strip('"\'')  # Remove quotes if present

                    if Path(path).exists():
                        macro_info.found = True
                        macro_info.resolved_path = path
                        self.resolved_paths[macro_ref] = path

                        # Parse the macro
                        macro_info.workflow = self._parse_macro(path)
                        print(f"Macro resolved: {path}")
                        return macro_info
                    else:
                        print(f"File not found: {path}")
                        continue

                elif choice == "2":
                    directory = input("Enter directory path: ").strip()
                    directory = directory.strip('"\'')

                    if Path(directory).exists() and Path(directory).is_dir():
                        self.add_search_directory(directory)

                        # Try to find the macro again
                        resolved = self._search_for_macro(macro_ref, workflow)
                        if resolved:
                            macro_info.found = True
                            macro_info.resolved_path = str(resolved)
                            self.resolved_paths[macro_ref] = str(resolved)
                            macro_info.workflow = self._parse_macro(str(resolved))
                            print(f"Macro found: {resolved}")
                            return macro_info
                        else:
                            print(f"Macro not found in {directory}")
                            print("Directory added to search paths for future lookups.")
                            # Continue the prompt loop
                            continue
                    else:
                        print(f"Invalid directory: {directory}")
                        continue

                elif choice == "3":
                    self.skip_macros.add(macro_ref)
                    print(f"Skipping macro: {macro_name}")
                    return macro_info

                elif choice == "4":
                    self.skip_all = True
                    self.skip_macros.add(macro_ref)
                    print("Skipping all missing macros")
                    return macro_info

                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")

            except KeyboardInterrupt:
                print("\nSkipping macro resolution")
                return macro_info
            except EOFError:
                # Non-interactive environment
                self.skip_macros.add(macro_ref)
                return macro_info

    def _parse_macro(self, file_path: str) -> Optional[AlteryxWorkflow]:
        """Parse a macro file."""
        try:
            parser = AlteryxParser()
            return parser.parse(file_path)
        except Exception as e:
            print(f"Warning: Could not parse macro {file_path}: {e}")
            return None


class MacroInventory:
    """Tracks all macros used across multiple workflows."""

    def __init__(self):
        self.macros: Dict[str, MacroInfo] = {}
        self.usage: Dict[str, List[str]] = {}  # macro_name -> list of workflows using it

    def add_macro(self, macro_info: MacroInfo, workflow_name: str) -> None:
        """Add a macro to the inventory."""
        key = macro_info.name

        if key not in self.macros:
            self.macros[key] = macro_info
            self.usage[key] = []

        self.usage[key].append(workflow_name)

    def get_shared_macros(self) -> List[MacroInfo]:
        """Get macros used by multiple workflows."""
        return [
            self.macros[name]
            for name, workflows in self.usage.items()
            if len(workflows) > 1
        ]

    def get_missing_macros(self) -> List[MacroInfo]:
        """Get macros that were not found."""
        return [m for m in self.macros.values() if not m.found]

    def get_summary(self) -> Dict:
        """Get a summary of macro usage."""
        return {
            "total_macros": len(self.macros),
            "found": sum(1 for m in self.macros.values() if m.found),
            "missing": sum(1 for m in self.macros.values() if not m.found),
            "shared": len(self.get_shared_macros()),
            "usage": {name: len(workflows) for name, workflows in self.usage.items()},
        }
