import { createContext } from 'react'

// The menu rail's collapsed state, published by App and read by pages to
// trade card-grid density (the rail auto-collapses on a narrow viewport and
// the trigger flips it by hand — either way this is the signal the operator
// called 菜单栏折叠). On the cluster page: data-plane PeerCards run 2 per row
// and drop to 1 collapsed; control-plane PeerCards run 3 per row and drop
// to 2. Default false matches a fresh wide viewport: a page rendered
// outside the provider (tests) sees the expanded grid.
export const RailContext = createContext(false)
