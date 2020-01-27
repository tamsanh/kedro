# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""BioSequenceDataSet loads and saves data to/from bio-sequence objects to
file.
"""
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, Dict, List

import fsspec
from Bio import SeqIO

from kedro.contrib.io import DefaultArgumentsMixIn
from kedro.io import AbstractDataSet
from kedro.io.core import get_filepath_str, get_protocol_and_path


class BioSequenceDataSet(DefaultArgumentsMixIn, AbstractDataSet):
    r"""``BioSequenceDataSet`` loads and saves data to a sequence file.

    Example:
    ::

        >>> from kedro.contrib.io.bioinformatics import BioSequenceDataSet
        >>> from io import StringIO
        >>> from Bio import SeqIO
        >>>
        >>> data = ">Alpha\nACCGGATGTA\n>Beta\nAGGCTCGGTTA\n"
        >>> raw_data = []
        >>> for record in SeqIO.parse(StringIO(data), "fasta"):
        >>>     raw_data.append(record)
        >>>
        >>> data_set = BioSequenceDataSet(filepath="ls_orchid.fasta",
        >>>                               load_args={"format": "fasta"},
        >>>                               save_args={"format": "fasta"})
        >>> data_set.save(raw_data)
        >>> sequence_list = data_set.load()
        >>>
        >>> assert raw_data[0].id == sequence_list[0].id
        >>> assert raw_data[0].seq == sequence_list[0].seq

    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        filepath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        credentials: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
    ) -> None:
        """
        Creates a new instance of ``BioSequenceDataSet`` pointing
        to a concrete filepath.

        Args:
            filepath: path to sequence file prefixed with a protocol like `s3://`.
                If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
            load_args: Options for parsing sequence files by Biopython ``SeqIO.parse()``.
            save_args: file format supported by Biopython ``SeqIO.write()``.
                E.g. `{"format": "fasta"}`.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Extra arguments to pass into underlying filesystem class.
                E.g. for ``GCSFileSystem`` class: `{"project": "my-project", ...}`.

        Note: Here you can find all supported file formats: https://biopython.org/wiki/SeqIO
        """
        super().__init__(load_args, save_args)

        _fs_args = deepcopy(fs_args) or {}
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath)

        self._filepath = PurePosixPath(path)
        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
            protocol=self._protocol,
            load_args=self._load_args,
            save_args=self._save_args,
        )

    def _load(self) -> List:
        load_path = get_filepath_str(self._filepath, self._protocol)
        with self._fs.open(load_path, mode="r") as fs_file:
            return list(SeqIO.parse(handle=fs_file, **self._load_args))

    def _save(self, data: List) -> None:
        save_path = get_filepath_str(self._filepath, self._protocol)

        with self._fs.open(save_path, mode="w") as fs_file:
            SeqIO.write(data, handle=fs_file, **self._save_args)

    def _exists(self) -> bool:
        load_path = get_filepath_str(self._filepath, self._protocol)
        return self._fs.exists(load_path)

    def _release(self) -> None:
        self.invalidate_cache()

    def invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
