
import {usePluginData} from '@docusaurus/useGlobalData';
import {useLocation} from '@docusaurus/router';

/**
 * This is a workaround for docusaurus not making links versioned by default.
 * Use VersionedLink instead of accessing this directly.
 * Since we added a new instance for cloud, now useVersionedPath must have paths
 * relative to the root, so we can identify to which instance it is pointing to.
 * 
 * Example use: `const path = useVersionedPath('/docs/my-docs')`
 *   outputs `/docs/my-doc` if the current version is "current"
 *   outputs `/docs/1.1.1/my-doc` if the current version is "1.1.1"
 */
export const useVersionedPath = (path) => {
    console.log(path)
    if (!path.startsWith('/docs') && !path.startsWith('/cloud')) {
        throw Error("paths to useVersionedLink must be root-relative")
    }
    const {pathname} = useLocation();
    const {versions} = usePluginData('docusaurus-plugin-content-docs');
    for (const version of Object.values(versions)) {
        if (version.path === '/docs' || version.path === '/cloud') {
            // Skip the "current" version, since it matches all cases
            continue;
        }

        if (pathname.startsWith(`${version.path}/`)) {
            const pathWithoutInstance = path.replace("/docs/","/").replace("/cloud/","/")
            return `${version.path}${pathWithoutInstance}`
        }
    }
    // fall back to current docs, since we skipped it earlier
    return `${path}`
}
