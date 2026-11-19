// A workspace Python module path: relative, normalized and inside the
// workspace. Shared by the tenant gateway and the launcher core.
export function isStoragePath(path) {
  return typeof path === 'string' &&
    path.length >= 4 &&
    path.endsWith('.py') &&
    !path.startsWith('/') &&
    !path.includes('\\') &&
    !path.includes('\0') &&
    !path.split('/').some((part) => part === '' || part === '.' || part === '..' || part === '.git');
}
