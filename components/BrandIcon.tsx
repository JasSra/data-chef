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
    // Check if className is a hex color (starts with #) or a Tailwind class
    const isHexColor = className.startsWith('#')
    const style = isHexColor 
      ? { fontSize: `${size}px`, lineHeight: 1, color: className }
      : { fontSize: `${size}px`, lineHeight: 1 }
    
    return (
      <i 
        className={`${brandClass} ${isHexColor ? '' : className}`.trim()} 
        style={style} 
        aria-hidden="true" 
      />
    )
  }
  if (!Icon) return null
  return <Icon size={size} className={className} />
}
