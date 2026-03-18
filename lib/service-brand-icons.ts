/**
 * Font Awesome brand icon mapping for API services and connectors.
 * Uses FA 6 class names — loaded via CDN in layout.tsx.
 *
 * The matcher tries: exact baseUrl domain → keyword in name → fallback.
 */

interface BrandMatch {
  faClass: string
  color: string
}

/** Domain → brand icon */
const DOMAIN_MAP: Record<string, BrandMatch> = {
  'github.com':               { faClass: 'fa-brands fa-github',           color: 'text-zinc-200' },
  'api.github.com':           { faClass: 'fa-brands fa-github',           color: 'text-zinc-200' },
  'gitlab.com':               { faClass: 'fa-brands fa-gitlab',           color: 'text-orange-400' },
  'bitbucket.org':            { faClass: 'fa-brands fa-bitbucket',        color: 'text-blue-400' },
  'api.stripe.com':           { faClass: 'fa-brands fa-stripe',           color: 'text-purple-400' },
  'stripe.com':               { faClass: 'fa-brands fa-stripe',           color: 'text-purple-400' },
  'api.twitter.com':          { faClass: 'fa-brands fa-x-twitter',        color: 'text-zinc-200' },
  'api.x.com':                { faClass: 'fa-brands fa-x-twitter',        color: 'text-zinc-200' },
  'graph.facebook.com':       { faClass: 'fa-brands fa-facebook',         color: 'text-blue-500' },
  'graph.microsoft.com':      { faClass: 'fa-brands fa-microsoft',        color: 'text-cyan-300' },
  'login.microsoftonline.com':{ faClass: 'fa-brands fa-microsoft',        color: 'text-cyan-300' },
  'management.azure.com':     { faClass: 'fa-brands fa-microsoft',        color: 'text-cyan-300' },
  'dev.azure.com':            { faClass: 'fa-brands fa-microsoft',        color: 'text-cyan-300' },
  'googleapis.com':           { faClass: 'fa-brands fa-google',           color: 'text-rose-400' },
  'maps.googleapis.com':      { faClass: 'fa-brands fa-google',           color: 'text-emerald-400' },
  'www.googleapis.com':       { faClass: 'fa-brands fa-google',           color: 'text-rose-400' },
  'firebaseio.com':           { faClass: 'fa-brands fa-google',           color: 'text-amber-400' },
  'api.slack.com':            { faClass: 'fa-brands fa-slack',            color: 'text-fuchsia-400' },
  'slack.com':                { faClass: 'fa-brands fa-slack',            color: 'text-fuchsia-400' },
  'discord.com':              { faClass: 'fa-brands fa-discord',          color: 'text-indigo-400' },
  'api.twilio.com':           { faClass: 'fa-brands fa-twilio',           color: 'text-rose-400' },  // custom icon below
  'api.spotify.com':          { faClass: 'fa-brands fa-spotify',          color: 'text-emerald-400' },
  'api.dropbox.com':          { faClass: 'fa-brands fa-dropbox',          color: 'text-blue-400' },
  'api.paypal.com':           { faClass: 'fa-brands fa-paypal',           color: 'text-blue-300' },
  'api.shopify.com':          { faClass: 'fa-brands fa-shopify',          color: 'text-emerald-400' },
  'api.wordpress.org':        { faClass: 'fa-brands fa-wordpress',        color: 'text-sky-300' },
  'api.figma.com':            { faClass: 'fa-brands fa-figma',            color: 'text-fuchsia-300' },
  'hub.docker.com':           { faClass: 'fa-brands fa-docker',           color: 'text-blue-400' },
  'registry.npmjs.org':       { faClass: 'fa-brands fa-npm',              color: 'text-rose-400' },
  'pypi.org':                 { faClass: 'fa-brands fa-python',           color: 'text-yellow-300' },
  'api.cloudflare.com':       { faClass: 'fa-brands fa-cloudflare',       color: 'text-orange-400' },
  'api.digitalocean.com':     { faClass: 'fa-brands fa-digital-ocean',    color: 'text-blue-400' },
  'api.heroku.com':           { faClass: 'fa-solid fa-h',                 color: 'text-purple-400' },
  'api.openai.com':           { faClass: 'fa-solid fa-robot',             color: 'text-emerald-300' },
  'api.anthropic.com':        { faClass: 'fa-solid fa-brain',             color: 'text-amber-300' },
  'api.linkedin.com':         { faClass: 'fa-brands fa-linkedin',         color: 'text-blue-400' },
  'api.instagram.com':        { faClass: 'fa-brands fa-instagram',        color: 'text-pink-400' },
  'api.reddit.com':           { faClass: 'fa-brands fa-reddit',           color: 'text-orange-400' },
  'api.youtube.com':          { faClass: 'fa-brands fa-youtube',          color: 'text-rose-500' },
  'www.reddit.com':           { faClass: 'fa-brands fa-reddit',           color: 'text-orange-400' },
  'api.twitch.tv':            { faClass: 'fa-brands fa-twitch',           color: 'text-purple-400' },
  'api.notion.com':           { faClass: 'fa-solid fa-n',                 color: 'text-zinc-200' },
  'api.trello.com':           { faClass: 'fa-brands fa-trello',           color: 'text-blue-400' },
  'api.atlassian.com':        { faClass: 'fa-brands fa-atlassian',        color: 'text-blue-500' },
  'jira.atlassian.com':       { faClass: 'fa-brands fa-jira',             color: 'text-blue-400' },
  'confluence.atlassian.com': { faClass: 'fa-brands fa-confluence',       color: 'text-blue-400' },
  'api.hubspot.com':          { faClass: 'fa-brands fa-hubspot',          color: 'text-orange-400' },
  'api.salesforce.com':       { faClass: 'fa-brands fa-salesforce',       color: 'text-blue-400' },
  'api.sendgrid.com':         { faClass: 'fa-solid fa-paper-plane',       color: 'text-blue-300' },
  'api.mailchimp.com':        { faClass: 'fa-brands fa-mailchimp',        color: 'text-amber-300' },
  'api.intercom.io':          { faClass: 'fa-brands fa-intercom',         color: 'text-blue-400' },
  'api.zendesk.com':          { faClass: 'fa-brands fa-zendesk',          color: 'text-emerald-400' },
  'petstore.swagger.io':      { faClass: 'fa-solid fa-paw',               color: 'text-emerald-400' },
  'httpbin.org':              { faClass: 'fa-solid fa-vial',              color: 'text-amber-300' },
  'jsonplaceholder.typicode.com': { faClass: 'fa-solid fa-database',      color: 'text-amber-300' },
}

/** Keyword (in service name or URL) → brand icon */
const KEYWORD_MAP: [RegExp, BrandMatch][] = [
  [/github/i,       { faClass: 'fa-brands fa-github',       color: 'text-zinc-200' }],
  [/gitlab/i,       { faClass: 'fa-brands fa-gitlab',       color: 'text-orange-400' }],
  [/bitbucket/i,    { faClass: 'fa-brands fa-bitbucket',    color: 'text-blue-400' }],
  [/stripe/i,       { faClass: 'fa-brands fa-stripe',       color: 'text-purple-400' }],
  [/twitter|x\.com/i, { faClass: 'fa-brands fa-x-twitter', color: 'text-zinc-200' }],
  [/facebook|meta/i,{ faClass: 'fa-brands fa-facebook',     color: 'text-blue-500' }],
  [/microsoft|azure|entra/i, { faClass: 'fa-brands fa-microsoft', color: 'text-cyan-300' }],
  [/google|gcp|bigquery|firebase/i, { faClass: 'fa-brands fa-google', color: 'text-rose-400' }],
  [/amazon|aws|s3/i, { faClass: 'fa-brands fa-aws',         color: 'text-amber-400' }],
  [/slack/i,        { faClass: 'fa-brands fa-slack',         color: 'text-fuchsia-400' }],
  [/discord/i,      { faClass: 'fa-brands fa-discord',       color: 'text-indigo-400' }],
  [/spotify/i,      { faClass: 'fa-brands fa-spotify',       color: 'text-emerald-400' }],
  [/dropbox/i,      { faClass: 'fa-brands fa-dropbox',       color: 'text-blue-400' }],
  [/paypal/i,       { faClass: 'fa-brands fa-paypal',        color: 'text-blue-300' }],
  [/shopify/i,      { faClass: 'fa-brands fa-shopify',       color: 'text-emerald-400' }],
  [/wordpress/i,    { faClass: 'fa-brands fa-wordpress',     color: 'text-sky-300' }],
  [/docker/i,       { faClass: 'fa-brands fa-docker',        color: 'text-blue-400' }],
  [/npm/i,          { faClass: 'fa-brands fa-npm',           color: 'text-rose-400' }],
  [/python|django|flask/i, { faClass: 'fa-brands fa-python', color: 'text-yellow-300' }],
  [/node|express/i, { faClass: 'fa-brands fa-node-js',       color: 'text-emerald-400' }],
  [/java|spring/i,  { faClass: 'fa-brands fa-java',          color: 'text-orange-400' }],
  [/php|laravel/i,  { faClass: 'fa-brands fa-php',           color: 'text-indigo-400' }],
  [/rust/i,         { faClass: 'fa-brands fa-rust',           color: 'text-orange-300' }],
  [/angular/i,      { faClass: 'fa-brands fa-angular',        color: 'text-rose-400' }],
  [/react/i,        { faClass: 'fa-brands fa-react',          color: 'text-cyan-300' }],
  [/vue/i,          { faClass: 'fa-brands fa-vuejs',          color: 'text-emerald-400' }],
  [/cloudflare/i,   { faClass: 'fa-brands fa-cloudflare',     color: 'text-orange-400' }],
  [/digital.?ocean/i, { faClass: 'fa-brands fa-digital-ocean', color: 'text-blue-400' }],
  [/openai|chatgpt/i, { faClass: 'fa-solid fa-robot',        color: 'text-emerald-300' }],
  [/anthropic|claude/i, { faClass: 'fa-solid fa-brain',      color: 'text-amber-300' }],
  [/linkedin/i,     { faClass: 'fa-brands fa-linkedin',       color: 'text-blue-400' }],
  [/instagram/i,    { faClass: 'fa-brands fa-instagram',      color: 'text-pink-400' }],
  [/reddit/i,       { faClass: 'fa-brands fa-reddit',         color: 'text-orange-400' }],
  [/youtube/i,      { faClass: 'fa-brands fa-youtube',        color: 'text-rose-500' }],
  [/twitch/i,       { faClass: 'fa-brands fa-twitch',         color: 'text-purple-400' }],
  [/jira/i,         { faClass: 'fa-brands fa-jira',            color: 'text-blue-400' }],
  [/confluence/i,   { faClass: 'fa-brands fa-confluence',      color: 'text-blue-400' }],
  [/atlassian/i,    { faClass: 'fa-brands fa-atlassian',       color: 'text-blue-500' }],
  [/hubspot/i,      { faClass: 'fa-brands fa-hubspot',         color: 'text-orange-400' }],
  [/salesforce/i,   { faClass: 'fa-brands fa-salesforce',      color: 'text-blue-400' }],
  [/mailchimp/i,    { faClass: 'fa-brands fa-mailchimp',       color: 'text-amber-300' }],
  [/intercom/i,     { faClass: 'fa-brands fa-intercom',        color: 'text-blue-400' }],
  [/zendesk/i,      { faClass: 'fa-brands fa-zendesk',         color: 'text-emerald-400' }],
  [/figma/i,        { faClass: 'fa-brands fa-figma',            color: 'text-fuchsia-300' }],
  [/notion/i,       { faClass: 'fa-solid fa-n',                 color: 'text-zinc-200' }],
  [/trello/i,       { faClass: 'fa-brands fa-trello',           color: 'text-blue-400' }],
  [/swagger|openapi/i, { faClass: 'fa-solid fa-book-open',     color: 'text-emerald-400' }],
  [/graphql/i,      { faClass: 'fa-solid fa-diagram-project',   color: 'text-pink-400' }],
  [/rest|api/i,     { faClass: 'fa-solid fa-plug-circle-bolt',  color: 'text-indigo-400' }],
  [/postgres|postgresql/i, { faClass: 'fa-solid fa-database',      color: '#336791' }],
  [/mysql|mariadb/i, { faClass: 'fa-solid fa-database',            color: '#00758F' }],
  [/mongo|mongodb/i, { faClass: 'fa-solid fa-leaf',                color: '#47A248' }],
  [/redis/i,        { faClass: 'fa-solid fa-cubes-stacked',       color: '#DC382D' }],
  [/rabbitmq/i,     { faClass: 'fa-solid fa-rabbit',              color: '#FF6600' }],
  [/kafka/i,        { faClass: 'fa-solid fa-stream',              color: '#231F20' }],
  [/mqtt/i,         { faClass: 'fa-solid fa-tower-broadcast',     color: '#660066' }],
  [/elastic/i,      { faClass: 'fa-solid fa-magnifying-glass',    color: '#FEC514' }],
  [/datadog/i,      { faClass: 'fa-solid fa-dog',                color: 'text-purple-400' }],
  [/brisbane|gov\.au/i, { faClass: 'fa-solid fa-landmark',      color: 'text-amber-300' }],
]

const FALLBACK: BrandMatch = { faClass: 'fa-solid fa-globe', color: 'text-indigo-400' }

/**
 * Get the Font Awesome class + color for a service based on its name and baseUrl.
 */
export function getServiceBrandIcon(name: string, baseUrl: string): BrandMatch {
  // Try domain match first
  try {
    const domain = new URL(baseUrl).hostname
    // Try exact, then parent domain
    if (DOMAIN_MAP[domain]) return DOMAIN_MAP[domain]
    const parts = domain.split('.')
    if (parts.length > 2) {
      const parent = parts.slice(-2).join('.')
      if (DOMAIN_MAP[parent]) return DOMAIN_MAP[parent]
    }
  } catch { /* invalid url, try keywords */ }

  // Try keyword match on name + url
  const haystack = `${name} ${baseUrl}`
  for (const [re, match] of KEYWORD_MAP) {
    if (re.test(haystack)) return match
  }

  return FALLBACK
}

/** Connector type → brand icon (for sidebar/connections) */
export const CONNECTOR_BRAND_ICONS: Record<string, BrandMatch> = {
  postgresql:    { faClass: 'fa-solid fa-database',           color: '#336791' },
  mysql:         { faClass: 'fa-solid fa-database',           color: '#00758F' },
  mongodb:       { faClass: 'fa-solid fa-leaf',               color: '#47A248' },
  redis:         { faClass: 'fa-solid fa-cubes-stacked',      color: '#DC382D' },
  rabbitmq:      { faClass: 'fa-solid fa-rabbit',             color: '#FF6600' },
  mqtt:          { faClass: 'fa-solid fa-tower-broadcast',    color: '#660066' },
  kafka:         { faClass: 'fa-solid fa-stream',             color: '#231F20' },
  s3:            { faClass: 'fa-brands fa-aws',               color: 'text-amber-400' },
  bigquery:      { faClass: 'fa-brands fa-google',            color: 'text-rose-400' },
  elasticsearch: { faClass: 'fa-solid fa-magnifying-glass',   color: 'text-teal-400' },
  github:        { faClass: 'fa-brands fa-github',            color: 'text-zinc-200' },
  azuredevops:   { faClass: 'fa-brands fa-microsoft',         color: 'text-cyan-300' },
  appinsights:   { faClass: 'fa-brands fa-microsoft',         color: 'text-cyan-400' },
  azuremonitor:  { faClass: 'fa-brands fa-microsoft',         color: 'text-cyan-300' },
  azureb2c:      { faClass: 'fa-brands fa-microsoft',         color: 'text-teal-400' },
  azureentraid:  { faClass: 'fa-brands fa-microsoft',         color: 'text-sky-300' },
  datadog:       { faClass: 'fa-solid fa-dog',                color: 'text-purple-400' },
  http:          { faClass: 'fa-solid fa-plug-circle-bolt',   color: 'text-indigo-400' },
  webhook:       { faClass: 'fa-solid fa-tower-broadcast',    color: 'text-amber-400' },
  sftp:          { faClass: 'fa-solid fa-server',             color: 'text-blue-300' },
  file:          { faClass: 'fa-solid fa-file',               color: 'text-chef-muted' },
}
