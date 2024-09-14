<template>
    <speak-easy 
    @clickSuggestion="store.clickSuggestion" 
    @input="fetchResults"
    @submit-event="store.speak_easy_router" 
    :apiResponse="store.llm_output"
    :autoSuggestionData="store.autoSuggestionData" 
    :endOfStream="store.endOfStream">
    </speak-easy>
  
    <n-grid x-gap="0" cols="24"
      style="width:100vw;height:100px;padding-left: 0rem;background: transparent;z-index: 99;position: fixed;top:0px;padding-top:12px;left:0"
      class="header">
      <n-gi span="8"
        style="margin-top:auto;margin-bottom:auto;padding-left:5rem;max-width:1280px;">
        <span style="font-weight: 200">FULL</span> <span style="font-weight: 200">METAL <span
            style="font-weight: 400">WEAVIATE</span></span>
      </n-gi>
      <n-gi span="8" style="margin:auto;text-align:center;">
      </n-gi>
      <n-gi span="7" style="margin-top:auto;margin-bottom:auto;margin-right:0rem;">
      </n-gi>

      <n-gi span="24" style="margin: auto;">
        <div style="width: 100%;padding-top: 12px;">
          <n-button-group style="z-index:9;width:100%;justify-content:center;" id="dimensionButton">
            <n-button class="dsbutton" v-for="i in store.header_button" :key="i.display_name"
            @click="store.set_dimension(i.display_name)" round>
              <n-icon v-if="store.header_mode =='icon'" :component="i.icon" color="black" size="16"></n-icon>
              <span v-else>{{i.display_name}}</span>
            </n-button>
          </n-button-group>
        </div>
      </n-gi>
    </n-grid>
  </template>
  
  
  <script setup lang="ts">
  import axios from 'axios'
  import { ref, onMounted, watch } from 'vue'
  import { NGi, NGrid, NButton, NSpace, NButtonGroup, NIcon, NPopover } from 'naive-ui'
  import { Home28Regular, Add28Regular, ArrowSwap20Regular } from '@vicons/fluent'
  import { mainStore } from '@/stores/store'
  import { storeToRefs } from 'pinia'
//   import '@/assets/monokai-sublime.css'
  
  import speakEasy from './SpeakEasy.vue'
  
  const store = mainStore()
  let previousScrollTop: number;
    
  const manageNavBarAnimations = (header) => {
    const scrollTop = document.documentElement.scrollTop;
  
    if (!store.isProgrammaticScroll) {
      let newHeaderTop = (parseInt(header.style.top || "0") - (scrollTop - previousScrollTop))
      let newHeaderTop2 = Math.min(0, Math.max(-70, newHeaderTop));
      header.style.top = newHeaderTop2 + 'px';
    }
    previousScrollTop = scrollTop
  };
  
//   watch(() => store.dimension, (newValue, oldValue) => {
//     if (store.dimension === 'thingsspace') {
//       store.getHierarchy({ mapId: store.clickedItemId, ontologyUuid: store.ontologyUuidSelected }, undefined, true, 'openMap')
//     }
//   })
  
  onMounted(() => {
    const header = document.querySelector(".header");
    window.addEventListener("scroll", (event) => {
      manageNavBarAnimations(header)
    });
  
    let script = document.createElement('script');
    script.setAttribute('src', 'https://platform.twitter.com/widgets.js');
    script.setAttribute('async', '');
    document.body.appendChild(script);
  
  });
  
  /////////////// debounce search //////////////////////////////
  function debounce(func, timeout = 300) {
    let timer;
    return (...args) => {
      clearTimeout(timer);
      timer = setTimeout(() => { func.apply(this, args); }, timeout);
    }
  }
  
  const debouncedFetchResults = debounce(async () => {
    await axios
      .post(store.apiUrl + "api/searchbar/", {
        text: store.searchBox//'model'//query.value
      })
      .then((response) => {
        if (response.status === 200) {
          store.autoSuggestionData = response.data
  
        }
      })
      .catch((error) => {
        console.error(error);
      })
  }, 1000);
  
  function fetchResults(v: String) {
    store.searchBox = v
    debouncedFetchResults();
  }
  </script>
  