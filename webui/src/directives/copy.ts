import type { App, Directive } from 'vue'
import { $t } from '@/utils'

interface CopyHTMLElement extends HTMLElement {
  _copyText: string
}

export function install(app: App) {
  const { isSupported, copy } = useClipboard()
  const permissionWrite = usePermission('clipboard-write')

  function clipboardEnable() {
    if (!isSupported.value) {
      window.$message.error('Your browser does not support Clipboard API')
      return false
    }

    if (permissionWrite.value !== 'granted') {
      window.$message.error('Currently not permitted to use Clipboard API')
      return false
    }
    return true
  }

  function copyHandler(this: any) {
    if (!clipboardEnable())
      return
    copy(this._copyText)
    window.$message.success($t('components.copyText.message'))
  }

  function updataClipboard(el: CopyHTMLElement, text: string) {
    el._copyText = text
    el.addEventListener('click', copyHandler)
  }

  const copyDirective: Directive<CopyHTMLElement, string> = {
    mounted(el, binding) {
      updataClipboard(el, binding.value)
    },
    updated(el, binding) {
      updataClipboard(el, binding.value)
    },
    unmounted(el) {
      el.removeEventListener('click', copyHandler)
    },
  }
  app.directive('copy', copyDirective)
}
