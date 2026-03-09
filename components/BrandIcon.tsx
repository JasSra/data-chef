'use client'

import type { ElementType } from 'react'

interface BrandIconProps {
  icon?: ElementType
  brandClass?: string
  size?: number
  className?: string
}

export default function BrandIcon({ icon: Icon, brandClass, size = 16, className = '' }: BrandIconProps) {
  if (brandClass) {
    return <i className={`${brandClass} ${className}`.trim()} style={{ fontSize: `${size}px`, lineHeight: 1 }} aria-hidden="true" />
  }
  if (!Icon) return null
  return <Icon size={size} className={className} />
}
