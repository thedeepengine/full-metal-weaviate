import { ref, computed } from 'vue'
import { defineStore } from 'pinia'
import { List28Regular, ArrowRouting20Filled, Map24Regular, GridDots28Regular } from '@vicons/fluent'

export const mainStore = defineStore('store', () => {
  const dimension = ref('hierarchy')
  
  const header_button = ref(
    [{display_name: 'Hierarchy',
    icon: Map24Regular},
    {display_name: 'Network',
    icon: ArrowRouting20Filled},
    {display_name: 'Things Space',
    icon: GridDots28Regular},
    {display_name: 'GraphQL',
    icon: List28Regular}]
)

const header_mode = ref('icon')
const speak_easy_router = ref(null)
const autoSuggestionData = ref(null)
const endOfStream = ref(null)
const llm_output = ref(null)
const clickSuggestion = ref(null)
const isProgrammaticScroll=ref(false)
const clickedItemId=ref(null)
const ontologyUuidSelected = ref(null)
const apiUrl = ref(null)
const searchBox = ref(null)

  return { 
    header_button, 
    dimension, 
    speak_easy_router, 
    autoSuggestionData, 
    endOfStream, 
    llm_output, 
    clickSuggestion,
    isProgrammaticScroll,
    clickedItemId,
    ontologyUuidSelected,
    apiUrl, 
    searchBox,
    header_mode }
})
