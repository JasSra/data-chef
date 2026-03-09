import { ShieldCheck, Building2, Clock3, Cpu, ExternalLink } from 'lucide-react'
import { getAppInfo } from '@/lib/app-info'

export default function AboutPage() {
  const appInfo = getAppInfo()
  const headline = appInfo.branding.aboutHeadline || 'Data operations workspace'
  const aboutBody = appInfo.branding.aboutBody || 'Ingest, query, and transform operational data from APIs, databases, and observability systems.'
  const builtLabel = new Date(appInfo.builtAt).toLocaleString('en-AU', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })

  return (
    <div className="min-h-full px-6 py-8 md:px-10">
      <div className="mx-auto max-w-5xl space-y-8">
        <section className="overflow-hidden rounded-[28px] border border-chef-border bg-[linear-gradient(135deg,rgba(17,27,34,0.98),rgba(8,14,18,0.96))] shadow-[0_30px_80px_rgba(0,0,0,0.25)]">
          <div className="grid gap-8 px-8 py-10 md:grid-cols-[1.4fr_0.8fr] md:px-10">
            <div>
              <div className="inline-flex items-center gap-2 rounded-full border border-cyan-400/20 bg-cyan-400/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.2em] text-cyan-300">
                <ShieldCheck size={12} />
                {appInfo.attribution.parentCompany} portfolio product
              </div>
              <h1 className="mt-5 font-display text-4xl font-bold tracking-tight text-chef-text md:text-5xl">
                {appInfo.name}
              </h1>
              <p className="mt-4 max-w-2xl text-sm leading-7 text-chef-text-dim">
                {headline}. {aboutBody}
              </p>
              <div className="mt-6 flex flex-wrap gap-3">
                <a
                  href={appInfo.attribution.url}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-2 rounded-xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-2 text-sm font-medium text-cyan-200 hover:bg-cyan-400/15 transition-colors"
                >
                  Visit {appInfo.attribution.parentCompany}
                  <ExternalLink size={14} />
                </a>
              </div>
            </div>
            <div className="rounded-[24px] border border-chef-border bg-chef-card/80 p-6 backdrop-blur">
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-chef-muted">Release information</div>
              <div className="mt-5 space-y-4">
                <div>
                  <div className="text-[11px] text-chef-muted">Version</div>
                  <div className="mt-1 font-display text-2xl font-semibold text-chef-text">v{appInfo.version}</div>
                </div>
                <div>
                  <div className="text-[11px] text-chef-muted">Built</div>
                  <div className="mt-1 text-sm text-chef-text">{builtLabel}</div>
                </div>
                <div>
                  <div className="text-[11px] text-chef-muted">Owner</div>
                  <div className="mt-1 text-sm text-chef-text">{appInfo.attribution.parentCompany}</div>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section className="grid gap-5 md:grid-cols-3">
          {[
            {
              icon: Building2,
              title: 'Parent company',
              body: `${appInfo.attribution.parentCompany} provides the default platform branding and support surface for this tenant.`,
            },
            {
              icon: Cpu,
              title: `What ${appInfo.name} does`,
              body: `${appInfo.name} connects APIs and databases, builds datasets, runs queries, and orchestrates repeatable pipelines.`,
            },
            {
              icon: Clock3,
              title: 'Release visibility',
              body: `Version and build time are surfaced directly in the app shell so operators can confirm what is running in ${appInfo.tenant.slug}.`,
            },
          ].map(item => (
            <div key={item.title} className="rounded-[22px] border border-chef-border bg-chef-card/80 p-6">
              <item.icon size={18} className="text-cyan-300" />
              <h2 className="mt-4 font-display text-xl font-semibold text-chef-text">{item.title}</h2>
              <p className="mt-2 text-sm leading-7 text-chef-text-dim">{item.body}</p>
            </div>
          ))}
        </section>
      </div>
    </div>
  )
}
