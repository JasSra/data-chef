import type { Metadata } from 'next'
import { Inter, JetBrains_Mono, Space_Grotesk } from 'next/font/google'
import './globals.css'
import AppShell from '@/components/AppShell'
import { ThemeProvider } from '@/components/ThemeProvider'
import { SettingsProvider } from '@/components/SettingsProvider'
import { getAppInfo } from '@/lib/app-info'

const inter = Inter({ subsets: ['latin'], variable: '--font-inter', display: 'swap' })
const mono  = JetBrains_Mono({ subsets: ['latin'], variable: '--font-mono', display: 'swap' })
const display = Space_Grotesk({ subsets: ['latin'], variable: '--font-display', display: 'swap' })

export async function generateMetadata(): Promise<Metadata> {
  const appInfo = getAppInfo()
  return {
    title: appInfo.branding.productName,
    description: appInfo.branding.aboutBody || 'Ingest, query, and transform operational data',
    icons: appInfo.branding.faviconUrl ? {
      icon: appInfo.branding.faviconUrl,
      shortcut: appInfo.branding.faviconUrl,
      apple: appInfo.branding.faviconUrl,
    } : undefined,
  }
}

// Runs synchronously before CSS paints — prevents white flash on light theme load
const themeScript = `(function(){try{var t=localStorage.getItem('chef-theme');if(t==='light')document.documentElement.classList.add('light');}catch(e){}})()`

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${inter.variable} ${mono.variable} ${display.variable}`}>
      <head>
        <script dangerouslySetInnerHTML={{ __html: themeScript }} />
        {/* Font Awesome 6 - Free + Brands for service icons */}
        <link
          rel="stylesheet"
          href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.7.2/css/all.min.css"
          integrity="sha512-Evv84Mr4kqVGRNSgIGL/F/aIDqQb7xQ2vcrdIwxfjThSH8CSR7PBEakCr51Ck+w+/U6swU2Im1vVX0SVk9ABhg=="
          crossOrigin="anonymous"
          referrerPolicy="no-referrer"
        />
      </head>
      <body className="font-sans bg-chef-bg text-chef-text">
        <ThemeProvider>
          <SettingsProvider>
            <AppShell>{children}</AppShell>
          </SettingsProvider>
        </ThemeProvider>
      </body>
    </html>
  )
}
