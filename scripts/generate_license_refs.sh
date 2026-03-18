$env:PYTHONIOENCODING="utf-8"
pip-licenses --format=markdown --with-urls --with-authors > DEPENDENCY_LICENSES.md
pip-licenses --with-license-file --no-license-path --format=plain --output-file licenses/THIRD_PARTY_LICENSES.txt