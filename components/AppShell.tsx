'use client'

import { usePathname } from 'next/navigation'
import Sidebar from './Sidebar'
import TopBar from './TopBar'

export default function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname()

  return (
    <div className="flex h-screen overflow-hidden bg-chef-bg">
      <Sidebar pathname={pathname} />
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        <TopBar pathname={pathname} />
        <main className="flex-1 overflow-auto">
          {children}
        </main>
      </div>
    </div>
  )
}
