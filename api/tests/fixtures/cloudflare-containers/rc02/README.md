# Published Containers Signature Fixture

`manifest.json`, `manifest.sigstore.json`, and `container-registry.mjs` are
unmodified [rc02 release assets](https://github.com/MTG-Thomas/bifrost-upstream/releases/tag/v2.0.0-rc.2026112402).
The signer and Workers manifest are from the API image for source revision
`85e761039753a4421e872f40cda8c792e3fc5e4c`, pinned by digest
`sha256:2942e975682738fb53cc0dffd8e7ae6365f5d1f8f42e68b74e558b5ba857411c`.

`trusted-root.json` is the public `trusted_root.json` authenticated and
cached by Cosign v2.4.1 during Sigstore TUF initialization. Its SHA-256 is
`6494e21ea73fa7ee769f85f57d5a3e6a08725eae1e38c755fc3517c9e6bc0b66`.
The snapshot keeps historical fixture verification offline. The runtime
obtains its own authenticated target from Sigstore TUF.
