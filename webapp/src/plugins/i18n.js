import { createI18n } from 'vue-i18n'

export function getLocalLanguage() {
    const myLocale = localStorage.getItem('my_locale')
    if (myLocale) {
        return myLocale
    }
    const localName = navigator.language.indexOf('zh') !== -1 ? 'zh' : 'en'
    localStorage.setItem('my_locale', localName)
    return localName
}

const i18n = createI18n({
    locale: getLocalLanguage(),
    messages: {
        'zh': require('@/assets/i18n/languages/zh_CN.js'),
        'en': require('@/assets/i18n/languages/en_US.js')
    }
})

export default i18n