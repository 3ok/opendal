from __future__ import annotations

from typing import Any

import opendal
import fsspec


class OpenDALFileSystem(fsspec.AbstractFileSystem):
    protocol = "opendal"
    
    def __init__(self, scheme: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._scheme = scheme
        self._op = opendal.Operator(scheme, **kwargs)

    @property
    def fsid(self) -> str:
        return self._scheme
    
    def mkdir(
        self,
        path: str,
        create_parents: bool = True,
        **kwargs: Any
    ) -> None:
        self._op.create_dir(path)
    
    def makedirs(
        self,
        path: str,
        exist_ok: bool = False,
    ) -> None:
        self._op.create_dir(path)
    
    def rmdir(self, path: str) -> None:
        self._op.remove_all(path)
    
    def ls(self, path: str, detail: bool = False, **kwargs: Any) -> list[str] | list[dict[str, Any]]:
        if detail:
            return [
                self.info(entry.path)
                for entry in self._op.list(path)
            ]
        else:
            return [
                entry.path
                for entry in self._op.list(path)
            ]
    
    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        path = self._strip_protocol(path)
        metadata = self._op.stat(path)
        return {
            "name": path,
            "content_disposition": metadata.content_disposition,
            "size": metadata.content_length,
            "content_md5": metadata.content_md5,
            "content_type": metadata.content_type,
            "mode": metadata.mode,
        }
        
    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        self._op.copy(path1, path2)
    
    def isfile(self, path: str) -> bool:
        try:
            metadata = self._op.stat(path)
        except opendal.exceptions.NotFound:
            return False
        else:
            return metadata.mode.is_file()
    
    def isdir(self, path: str) -> bool:
        metadata = self._op.stat(path)
        return metadata.mode.is_dir()

    def rm_file(self, path: str) -> None:
        self._op.delete(path)

    def touch(self, path: str, **kwargs: Any) -> None:
        self._op.write(path, b"")
    
    def _open(self, path, mode="rb", block_size=None, autocommit=True, cache_options=None, **kwargs):
        return self._op.open(path, mode)

    def glob(self, path, maxdepth=None, **kwargs):
        return super().glob(path, maxdepth, **kwargs)