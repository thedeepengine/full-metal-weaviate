import { ref, computed, onMounted } from 'vue'
import { defineStore } from 'pinia'
import { List28Regular, ArrowRouting20Filled, Map24Regular, GridDots28Regular } from '@vicons/fluent'

export const mainStore = defineStore('store', () => {
    const dimension = ref('hierarchy')
    const data = ref(null)


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


    function set_dimension(dimension) {
        dimension.value = dimension
    }

    function fetch_data(clt, request) {
        let clt = 'JeopardyQuestion'
        let request = ''
        axios
        .post("/v1/api/query", {clt: clt, request: request})
        .then(response => {
            return response
        })
        
    }

    onMounted(() => {
        watch(() => store.dimension, (newValue, oldValue) => {
            console.log('WATCHER DIMENSION')
            fetch_data('ddd', 'gggg')
            // if (store.dimension === 'thingsspace') {
            // store.getHierarchy({ mapId: store.clickedItemId, ontologyUuid: store.ontologyUuidSelected }, undefined, true, 'watch DeepEngineMain.vue')
            // }
        })

    // initializeAxios()
    // if (access_token.value !== null) {
    //   axios.post('/api/validate_token/', { access_token:access_token.value })
    //   .then(response => {
    //     if (response.data === true) {
    //       setToken(access_token.value)
    //     }
    //   })
    //   .catch(error => {
    //       console.error('Token validation error:', error);
    //   });
    // } 
  });
  

  return { 
    header_button, 
    dimension, 
    set_dimension,


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
